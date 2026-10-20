package com.codecrafters.redis.raft;

import com.codecrafters.redis.rpc.RpcMessage;
import com.codecrafters.redis.rpc.RpcTransportClient;
import com.codecrafters.redis.rpc.RpcTransportServer;
import com.codecrafters.redis.storage.RedisStore;
import com.codecrafters.redis.storage.ValueEntry;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadLocalRandom;

/** One independently replicated Raft group hosted by a node. */
public final class RaftGroup implements AutoCloseable {
    private enum Role { FOLLOWER, CANDIDATE, LEADER }
    private static final int MAX_COMMAND_BYTES = 16 * 1024 * 1024;
    private final String groupId, nodeId;
    private final Map<String, InetSocketAddress> peers;
    private final Map<String, Long> nextIndexByPeer = new java.util.HashMap<>();
    private final RaftLog log;
    private final RaftMetadataStore metadata;
    private final RedisStore store;
    private final RpcTransportServer transport;
    private final ScheduledExecutorService scheduler;
    private long term, commitIndex, lastApplied;
    private long lastContactNanos, electionTimeoutNanos, lastHeartbeatNanos;
    private String votedFor;
    private volatile String leaderId;
    private volatile Role role = Role.FOLLOWER;
    private boolean running;
    private boolean maintenanceFrozen;

    public RaftGroup(String groupId, String nodeId, InetSocketAddress bindAddress,
                     Map<String, InetSocketAddress> peers, Path directory,
                     RedisStore store) throws IOException {
        this.groupId = require(groupId, "groupId");
        this.nodeId = require(nodeId, "nodeId");
        this.peers = new LinkedHashMap<>(Objects.requireNonNull(peers));
        if (this.peers.containsKey(nodeId)) throw new IllegalArgumentException("Peers must not contain this node");
        this.log = new RaftLog(directory.resolve("raft.log"));
        this.metadata = new RaftMetadataStore(directory.resolve("raft.meta"));
        RaftMetadataStore.State state = metadata.load();
        this.term = state.term(); this.votedFor = state.votedFor(); this.commitIndex = state.commitIndex();
        this.store = Objects.requireNonNull(store);
        if (commitIndex > log.lastIndex()) throw new IOException("Persisted commit index exceeds the recovered log");
        applyCommitted();
        this.transport = new RpcTransportServer(bindAddress, 4, 3000, this::handleRpc);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "raft-group-" + groupId + "-" + nodeId);
            thread.setDaemon(true); return thread;
        });
    }

    public synchronized void start() throws IOException {
        if (running) return;
        transport.start(); running = true; resetElectionDeadline();
        scheduler.scheduleAtFixedRate(this::tickSafely, 25, 25, TimeUnit.MILLISECONDS);
    }
    public int getPort() { return transport.getPort(); }
    public synchronized long getTerm() { return term; }
    public synchronized String getLeaderId() { return leaderId; }
    public synchronized boolean isLeader() { return role == Role.LEADER; }
    public synchronized long getCommitIndex() { return commitIndex; }

    /** Starts one election round. A majority vote is required to become leader. */
    public synchronized boolean startElection() throws IOException {
        role = Role.CANDIDATE; leaderId = null; term++; votedFor = nodeId;
        resetElectionDeadline(); metadata.save(term, votedFor, commitIndex);
        long electionTerm = term, lastIndex = log.lastIndex(), lastTerm = log.lastTerm();
        int votes = 1;
        for (Map.Entry<String, InetSocketAddress> peer : peers.entrySet()) {
            try {
                RpcMessage response = call(peer.getKey(), peer.getValue(), RpcMessage.Type.VOTE_REQUEST,
                        encode(out -> { out.writeLong(electionTerm); out.writeLong(lastIndex); out.writeLong(lastTerm); }));
                if (response.getType() != RpcMessage.Type.VOTE_RESPONSE) continue;
                DataInputStream in = input(response); long responseTerm = in.readLong(); boolean granted = in.readBoolean();
                if (responseTerm > term) { stepDown(responseTerm); return false; }
                if (responseTerm == electionTerm && granted) votes++;
            } catch (IOException ignored) { }
        }
        if (term == electionTerm && votes >= majority()) {
            role = Role.LEADER; leaderId = nodeId;
            synchronized (nextIndexByPeer) {
                nextIndexByPeer.clear();
                for (String peerId : peers.keySet()) nextIndexByPeer.put(peerId, 1L);
            }
            RaftLogEntry noOp = log.append(term, new byte[] {0});
            int replicated = 1;
            for (Map.Entry<String, InetSocketAddress> peer : peers.entrySet()) {
                try { if (replicate(peer.getKey(), peer.getValue(), noOp)) replicated++; }
                catch (IOException ignored) { }
                if (term != electionTerm || role != Role.LEADER) return false;
            }
            if (replicated < majority()) { role = Role.FOLLOWER; leaderId = null; resetElectionDeadline(); return false; }
            commitIndex = noOp.getIndex(); metadata.save(term, votedFor, commitIndex); applyCommitted();
            notifyCommit(); lastHeartbeatNanos = System.nanoTime();
            return true;
        }
        role = Role.FOLLOWER;
        resetElectionDeadline();
        return false;
    }

    /** Appends and commits a SET only after its durable log entry reaches a majority. */
    public long put(String key, byte[] value) throws IOException {
        ensureWritesAllowed();
        byte[] command = encode(out -> { out.writeByte(1); out.writeUTF(key); out.writeInt(value.length); out.write(value); });
        return commitCommand(command);
    }

    /** Commits a set of non-expiring writes as one replicated state-machine entry. */
    public long putBatch(Map<String, byte[]> values) throws IOException {
        ensureWritesAllowed();
        Objects.requireNonNull(values, "values");
        if (values.isEmpty()) return commitIndex;
        byte[] command = encode(out -> {
            out.writeByte(4); out.writeInt(values.size());
            for (Map.Entry<String, byte[]> entry : values.entrySet()) {
                if (entry.getKey() == null || entry.getValue() == null)
                    throw new IllegalArgumentException("Batch key and value cannot be null");
                out.writeUTF(entry.getKey()); out.writeInt(entry.getValue().length); out.write(entry.getValue());
            }
        });
        if (command.length > MAX_COMMAND_BYTES) throw new IOException("Raft batch exceeds command size limit");
        return commitCommand(command);
    }

    public long setWithExpiry(String key, byte[] value, long expiryMillis) throws IOException {
        ensureWritesAllowed();
        if (expiryMillis <= 0) throw new IllegalArgumentException("expiry must be positive");
        long expiryAt = Math.addExact(System.currentTimeMillis(), expiryMillis);
        byte[] command = encode(out -> { out.writeByte(3); out.writeUTF(key); out.writeLong(expiryAt); out.writeInt(value.length); out.write(value); });
        return commitCommand(command);
    }

    public boolean delete(String key) throws IOException {
        ensureWritesAllowed();
        byte[] command = encode(out -> { out.writeByte(2); out.writeUTF(key); });
        synchronized (this) {
            if (role == Role.LEADER) {
                boolean existed = store.exists(key);
                commitAsLeader(command);
                return existed;
            }
        }
        return forwardClientCommand(command).keyExisted();
    }

    private long commitCommand(byte[] command) throws IOException {
        synchronized (this) {
            if (maintenanceFrozen && (command.length == 0
                    || (command[0] != 7 && command[0] != 8 && command[0] != 9)))
                throw new IOException("Raft group is frozen for shard migration");
            if (role == Role.LEADER) return commitAsLeader(command);
        }
        return forwardClientCommand(command).index();
    }

    private synchronized void ensureWritesAllowed() throws IOException {
        if (maintenanceFrozen) throw new IOException("Raft group is frozen for shard migration");
    }

    /** Quorum-committed write fence used while a Multi-Raft shard migration is in progress. */
    public long setMaintenanceFrozen(boolean frozen) throws IOException {
        byte[] command = new byte[] {7, (byte) (frozen ? 1 : 0)};
        synchronized (this) {
            if (role != Role.LEADER) throw new IOException("Maintenance changes must be sent to the Raft leader");
            return commitAsLeader(command);
        }
    }

    /** Returns an expiry-preserving view of all live values after a linearizable read fence. */
    public Map<String, ValueEntry> snapshotEntriesWithReadIndex() throws IOException {
        synchronized (this) {
            if (role != Role.LEADER) throw new IOException("Snapshot enumeration must use the Raft leader");
            long index = readIndex();
            while (lastApplied < index) applyCommitted();
        }
        Map<String, ValueEntry> result = new LinkedHashMap<>();
        for (String key : store.keys("*")) {
            ValueEntry entry = store.getEntry(key);
            if (entry != null && !entry.isExpired()) result.put(key, entry);
        }
        return Map.copyOf(result);
    }

    /** Quorum-commits a value with its original absolute expiration timestamp. */
    public long putEntry(String key, ValueEntry value) throws IOException {
        Objects.requireNonNull(value, "value");
        if (key == null) throw new IllegalArgumentException("key cannot be null");
        byte[] bytes = value.getValue().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] command = encode(out -> {
            out.writeByte(8); out.writeUTF(key); out.writeLong(value.getExpiryTimestampMillis());
            out.writeInt(bytes.length); out.write(bytes);
        });
        return commitCommand(command);
    }

    /** Quorum-commits a delete (maintenance path; the group may be write-frozen). */
    public long deleteForMigration(String key) throws IOException {
        if (key == null) throw new IllegalArgumentException("key cannot be null");
        byte[] command = encode(out -> { out.writeByte(9); out.writeUTF(key); });
        synchronized (this) {
            if (role != Role.LEADER) return forwardClientCommand(command).index();
            return commitAsLeader(command);
        }
    }

    private long commitAsLeader(byte[] command) throws IOException {
        long writeTerm = term;
        RaftLogEntry entry = log.append(term, command);
        int replicated = 1;
        for (Map.Entry<String, InetSocketAddress> peer : peers.entrySet()) {
            try { if (replicate(peer.getKey(), peer.getValue(), entry)) replicated++; }
            catch (IOException ignored) { }
            if (term != writeTerm || role != Role.LEADER) throw new IOException("Leadership changed while replicating write");
        }
        if (replicated < majority()) throw new IOException("Raft write did not reach a majority");
        commitIndex = entry.getIndex(); metadata.save(term, votedFor, commitIndex); applyCommitted();
        notifyCommit();
        return commitIndex;
    }

    private ClientResult forwardClientCommand(byte[] command) throws IOException {
        String currentLeader = leaderId;
        InetSocketAddress address = currentLeader == null ? null : peers.get(currentLeader);
        if (address == null) throw new IOException("Leader is not known yet");
        RpcMessage response = call(currentLeader, address, RpcMessage.Type.REQUEST, command);
        if (response.getType() != RpcMessage.Type.RESPONSE) throw new IOException("Invalid Raft client response");
        DataInputStream in = input(response);
        boolean success = in.readBoolean(); long index = in.readLong(); boolean keyExisted = in.readBoolean();
        if (!success) throw new IOException(in.readUTF());
        return new ClientResult(index, keyExisted);
    }

    private DataInputStream forwardClientRead(byte[] command) throws IOException {
        String currentLeader = leaderId;
        InetSocketAddress address = currentLeader == null ? null : peers.get(currentLeader);
        if (address == null) throw new IOException("Leader is not known yet");
        RpcMessage response = call(currentLeader, address, RpcMessage.Type.REQUEST, command);
        if (response.getType() != RpcMessage.Type.RESPONSE) throw new IOException("Invalid Raft read response");
        DataInputStream in = input(response);
        boolean success = in.readBoolean(); in.readLong(); in.readBoolean();
        if (!success) throw new IOException(in.readUTF());
        return in;
    }

    private synchronized RpcMessage handleClientCommand(RpcMessage request) throws IOException {
        DataInputStream in = input(request);
        byte[] command = in.readAllBytes();
        if (command.length > 0 && (command[0] == 5 || command[0] == 6)) {
            try {
                if (role != Role.LEADER) throw new IOException("Not leader; leader=" + leaderId);
                DataInputStream read = new DataInputStream(new ByteArrayInputStream(command));
                int operation = read.readUnsignedByte(); String argument = read.readUTF();
                if (read.available() != 0) throw new IOException("Trailing bytes in forwarded read");
                long index = readIndex();
                if (operation == 5) {
                    byte[] value = get(argument);
                    return response(request, RpcMessage.Type.RESPONSE, encode(out -> {
                        out.writeBoolean(true); out.writeLong(index); out.writeBoolean(false);
                        out.writeInt(value == null ? -1 : value.length);
                        if (value != null) out.write(value);
                    }));
                }
                List<String> keys = store.keys(argument);
                return response(request, RpcMessage.Type.RESPONSE, encode(out -> {
                    out.writeBoolean(true); out.writeLong(index); out.writeBoolean(false);
                    out.writeInt(keys.size()); for (String key : keys) out.writeUTF(key);
                }));
            } catch (IOException failure) {
                String reason = failure.getMessage() == null ? "Raft read failed" : failure.getMessage();
                return response(request, RpcMessage.Type.RESPONSE,
                        encode(out -> { out.writeBoolean(false); out.writeLong(-1); out.writeBoolean(false); out.writeUTF(reason); }));
            }
        }
        try { validateWriteCommand(command); }
        catch (IOException invalid) {
            String reason = invalid.getMessage() == null ? "Invalid Raft command" : invalid.getMessage();
            return response(request, RpcMessage.Type.RESPONSE,
                    encode(out -> { out.writeBoolean(false); out.writeLong(-1); out.writeBoolean(false); out.writeUTF(reason); }));
        }
        boolean keyExisted = false;
        if (role == Role.LEADER && command.length > 0 && command[0] == 2) {
            DataInputStream deleteCommand = new DataInputStream(new ByteArrayInputStream(command));
            deleteCommand.readUnsignedByte(); keyExisted = store.exists(deleteCommand.readUTF());
        }
        try {
            if (role != Role.LEADER) throw new IOException("Not leader; leader=" + leaderId);
            if (maintenanceFrozen && command.length > 0 && command[0] != 7 && command[0] != 8 && command[0] != 9)
                throw new IOException("Raft group is frozen for shard migration");
            long index = commitCommand(command);
            boolean existed = keyExisted;
            return response(request, RpcMessage.Type.RESPONSE,
                    encode(out -> { out.writeBoolean(true); out.writeLong(index); out.writeBoolean(existed); }));
        } catch (IOException failure) {
            String reason = failure.getMessage() == null ? "Raft request failed" : failure.getMessage();
            return response(request, RpcMessage.Type.RESPONSE,
                    encode(out -> { out.writeBoolean(false); out.writeLong(-1); out.writeBoolean(false); out.writeUTF(reason); }));
        }
    }

    private record ClientResult(long index, boolean keyExisted) { }

    private static void validateWriteCommand(byte[] command) throws IOException {
        if (command.length == 0 || command.length > MAX_COMMAND_BYTES)
            throw new IOException("Invalid Raft command size");
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(command));
        int operation = in.readUnsignedByte();
        if (operation == 1 || operation == 3) {
            in.readUTF();
            if (operation == 3 && in.readLong() <= 0) throw new IOException("Invalid expiration timestamp");
            int length = in.readInt();
            if (length < 0 || length > MAX_COMMAND_BYTES) throw new IOException("Invalid Raft value length");
            if (in.readNBytes(length).length != length) throw new IOException("Truncated Raft value");
        } else if (operation == 2) {
            in.readUTF();
        } else if (operation == 4) {
            int count = in.readInt();
            if (count < 0 || count > 1_000_000) throw new IOException("Invalid batch count");
            for (int i = 0; i < count; i++) {
                in.readUTF(); int length = in.readInt();
                if (length < 0 || length > MAX_COMMAND_BYTES) throw new IOException("Invalid batch value length");
                if (in.readNBytes(length).length != length) throw new IOException("Truncated batch value");
            }
        } else if (operation == 7) {
            int frozen = in.readUnsignedByte();
            if (frozen > 1) throw new IOException("Invalid maintenance state");
        } else if (operation == 8) {
            in.readUTF();
            if (in.readLong() == 0) throw new IOException("Invalid migration expiration timestamp");
            int length = in.readInt();
            if (length < 0 || length > MAX_COMMAND_BYTES || in.readNBytes(length).length != length)
                throw new IOException("Invalid migration value");
        } else if (operation == 9) in.readUTF();
        else throw new IOException("Unknown state machine operation");
        if (in.available() != 0) throw new IOException("Trailing bytes in Raft command");
    }

    public synchronized byte[] get(String key) {
        String value = store.get(key);
        return value == null ? null : value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    /** Linearizable read fence: a majority must confirm this leader's current term. */
    public synchronized long readIndex() throws IOException {
        if (role != Role.LEADER) throw new IOException("Not leader; leader=" + leaderId);
        long readTerm = term; int confirmations = 1;
        for (Map.Entry<String, InetSocketAddress> peer : peers.entrySet()) {
            try { if (replicate(peer.getKey(), peer.getValue(), log.get(log.lastIndex()).orElse(null))) confirmations++; }
            catch (IOException ignored) { }
        }
        if (role != Role.LEADER || term != readTerm || confirmations < majority())
            throw new IOException("ReadIndex failed to confirm leadership with a majority");
        return commitIndex;
    }

    /** Returns the locally applied value without a freshness guarantee. */
    public synchronized byte[] getFollowerRead(String key) { return get(key); }

    /**
     * Serves an explicitly bounded-staleness read. The local applied index must
     * be no more than {@code maxCommitLag} behind a quorum-confirmed ReadIndex
     * observed from the leader. This is a log-index bound, not a wall-clock bound.
     * If the leader cannot confirm authority with a quorum, or the local replica
     * is too far behind, the read fails closed.
     */
    public byte[] getBoundedStalenessRead(String key, long maxCommitLag) throws IOException {
        if (maxCommitLag < 0) throw new IllegalArgumentException("maxCommitLag cannot be negative");
        String knownLeader;
        long requestTerm;
        synchronized (this) {
            if (role == Role.LEADER) return getWithReadIndex(key);
            knownLeader = leaderId;
            requestTerm = term;
        }
        if (knownLeader == null) throw new IOException("Cannot bound follower read without a known leader");
        InetSocketAddress address = peers.get(knownLeader);
        if (address == null) throw new IOException("Known leader is not a configured peer");
        RpcMessage response = call(knownLeader, address, RpcMessage.Type.READ_FRESHNESS_REQUEST,
                encode(out -> out.writeLong(requestTerm)));
        if (response.getType() != RpcMessage.Type.READ_FRESHNESS_RESPONSE)
            throw new IOException("Invalid follower read-index response");
        DataInputStream in = input(response);
        long responseTerm = in.readLong();
        boolean confirmed = in.readBoolean();
        long observedLeaderCommit = in.readLong();
        synchronized (this) {
            if (!confirmed || responseTerm != term || requestTerm != term || role != Role.FOLLOWER
                    || !knownLeader.equals(leaderId))
                throw new IOException("Leader could not confirm follower read position");
            if (lastApplied < observedLeaderCommit - maxCommitLag)
                throw new IOException("Follower is outside configured commit-index lag bound");
            return get(key);
        }
    }

    public byte[] getWithReadIndex(String key) throws IOException {
        if (role != Role.LEADER) {
            DataInputStream result = forwardClientRead(encode(out -> { out.writeByte(5); out.writeUTF(key); }));
            int length = result.readInt();
            if (length < 0) return null;
            if (length > MAX_COMMAND_BYTES) throw new IOException("Invalid forwarded read length");
            byte[] value = result.readNBytes(length);
            if (value.length != length) throw new IOException("Truncated forwarded read");
            return value;
        }
        long index = readIndex();
        while (lastApplied < index) applyCommitted();
        return get(key);
    }

    public List<String> keysWithReadIndex(String pattern) throws IOException {
        if (role != Role.LEADER) {
            DataInputStream result = forwardClientRead(encode(out -> { out.writeByte(6); out.writeUTF(pattern); }));
            int count = result.readInt();
            if (count < 0 || count > 1_000_000) throw new IOException("Invalid forwarded key count");
            java.util.ArrayList<String> keys = new java.util.ArrayList<>(count);
            for (int i = 0; i < count; i++) keys.add(result.readUTF());
            return List.copyOf(keys);
        }
        long index = readIndex();
        while (lastApplied < index) applyCommitted();
        return store.keys(pattern);
    }

    public synchronized List<String> keysFollowerRead(String pattern) { return store.keys(pattern); }

    public List<String> keysWithBoundedStaleness(String pattern, long maxCommitLag) throws IOException {
        getBoundedStalenessRead("", maxCommitLag); // perform the same freshness and authority check
        synchronized (this) { return store.keys(pattern); }
    }

    private boolean replicate(String peerId, InetSocketAddress address, RaftLogEntry entry) throws IOException {
        long throughIndex = entry == null ? 0 : entry.getIndex();
        long next;
        synchronized (nextIndexByPeer) { next = nextIndexByPeer.getOrDefault(peerId, 1L); }
        int retries = 0;
        while (next <= throughIndex) {
            long requestedIndex = next;
            RaftLogEntry item = log.get(requestedIndex)
                    .orElseThrow(() -> new IOException("Leader log is missing index " + requestedIndex));
            long previousIndex = item.getIndex() - 1;
            long previousTerm = log.get(previousIndex).map(RaftLogEntry::getTerm).orElse(0L);
            byte[] payload = encode(out -> {
                out.writeLong(term); out.writeLong(previousIndex);
                out.writeLong(previousTerm); out.writeLong(commitIndex); out.writeBoolean(true); writeEntry(out, item);
            });
            RpcMessage response = call(peerId, address, RpcMessage.Type.APPEND_ENTRIES_REQUEST, payload);
            if (response.getType() != RpcMessage.Type.APPEND_ENTRIES_RESPONSE) return false;
            DataInputStream in = input(response); long responseTerm = in.readLong(); boolean success = in.readBoolean();
            in.readLong(); // leader commit index (also present on negative acknowledgements)
            if (responseTerm > term) { stepDown(responseTerm); return false; }
            if (!success) {
                next = Math.max(1, next - 1);
                synchronized (nextIndexByPeer) { nextIndexByPeer.put(peerId, next); }
                if (++retries > throughIndex + 1) return false;
                continue;
            }
            next = item.getIndex() + 1;
            synchronized (nextIndexByPeer) { nextIndexByPeer.put(peerId, next); }
            retries = 0;
        }
        long previousTerm = log.get(throughIndex).map(RaftLogEntry::getTerm).orElse(0L);
        byte[] heartbeat = encode(out -> {
            out.writeLong(term); out.writeLong(throughIndex); out.writeLong(previousTerm);
            out.writeLong(commitIndex); out.writeBoolean(false);
        });
        RpcMessage response = call(peerId, address, RpcMessage.Type.APPEND_ENTRIES_REQUEST, heartbeat);
        if (response.getType() != RpcMessage.Type.APPEND_ENTRIES_RESPONSE) return false;
        DataInputStream in = input(response); long responseTerm = in.readLong(); boolean success = in.readBoolean();
        in.readLong(); // leader commit index
        if (responseTerm > term) { stepDown(responseTerm); return false; }
        if (success) synchronized (nextIndexByPeer) { nextIndexByPeer.put(peerId, throughIndex + 1); }
        else synchronized (nextIndexByPeer) { nextIndexByPeer.put(peerId, Math.max(1, throughIndex)); }
        return success;
    }

    private RpcMessage handleRpc(RpcMessage request) throws Exception {
        if (!request.getTargetNodeId().equals(nodeId)) throw new IOException("Wrong target node");
        if (!request.getSourceNodeId().equals(nodeId) && !peers.containsKey(request.getSourceNodeId()))
            throw new IOException("Unknown source node");
        DataInputStream envelope = input(request);
        String incomingGroup = envelope.readUTF();
        if (!groupId.equals(incomingGroup)) throw new IOException("Raft group mismatch");
        byte[] payload = envelope.readAllBytes();
        RpcMessage scopedRequest = new RpcMessage(request.getType(), request.getRequestId(),
                request.getSourceNodeId(), request.getTargetNodeId(), payload);
        if (request.getType() == RpcMessage.Type.VOTE_REQUEST) return handleVote(scopedRequest);
        if (request.getType() == RpcMessage.Type.APPEND_ENTRIES_REQUEST) return handleAppend(scopedRequest);
        if (request.getType() == RpcMessage.Type.READ_FRESHNESS_REQUEST) return handleReadFreshness(scopedRequest);
        if (request.getType() == RpcMessage.Type.REQUEST) return handleClientCommand(scopedRequest);
        throw new IOException("Unsupported Raft RPC");
    }

    private synchronized RpcMessage handleReadFreshness(RpcMessage request) throws IOException {
        DataInputStream in = input(request);
        long requesterTerm = in.readLong();
        if (in.available() != 0) throw new IOException("Malformed freshness request");
        boolean confirmed = false;
        long currentCommit = -1;
        if (role == Role.LEADER && term == requesterTerm) {
            try { currentCommit = readIndex(); confirmed = true; }
            catch (IOException noQuorum) { confirmed = false; }
        }
        long currentTerm = term;
        boolean result = confirmed;
        long confirmedCommit = currentCommit;
        return response(request, RpcMessage.Type.READ_FRESHNESS_RESPONSE, encode(out -> {
            out.writeLong(currentTerm); out.writeBoolean(result); out.writeLong(confirmedCommit);
        }));
    }

    private synchronized RpcMessage handleVote(RpcMessage request) throws IOException {
        DataInputStream in = input(request); long candidateTerm = in.readLong(), candidateIndex = in.readLong(), candidateLastTerm = in.readLong();
        if (candidateTerm > term) stepDown(candidateTerm);
        boolean upToDate = candidateLastTerm > log.lastTerm() ||
                candidateLastTerm == log.lastTerm() && candidateIndex >= log.lastIndex();
        boolean grant = candidateTerm == term && upToDate && (votedFor == null || votedFor.equals(request.getSourceNodeId()));
        if (grant) { votedFor = request.getSourceNodeId(); metadata.save(term, votedFor, commitIndex); leaderId = null; role = Role.FOLLOWER; resetElectionDeadline(); }
        long currentTerm = term; boolean result = grant;
        return response(request, RpcMessage.Type.VOTE_RESPONSE, encode(out -> { out.writeLong(currentTerm); out.writeBoolean(result); }));
    }

    private synchronized RpcMessage handleAppend(RpcMessage request) throws IOException {
        DataInputStream in = input(request); long leaderTerm = in.readLong(), previousIndex = in.readLong(), previousTerm = in.readLong(), leaderCommit = in.readLong();
        if (leaderTerm > term) stepDown(leaderTerm);
        boolean success = false;
        if (leaderTerm == term && previousIndex <= log.lastIndex() &&
                (previousIndex == 0 || log.get(previousIndex).map(e -> e.getTerm() == previousTerm).orElse(false))) {
            leaderId = request.getSourceNodeId(); role = Role.FOLLOWER;
            resetElectionDeadline();
            boolean hasEntry = in.readBoolean();
            if (hasEntry) {
                RaftLogEntry incoming = readEntry(in);
                if (incoming.getIndex() != previousIndex + 1) throw new IOException("Unexpected entry index");
                var existing = log.get(incoming.getIndex());
                if (existing.isPresent() && existing.get().getTerm() == incoming.getTerm()
                        && !existing.get().equals(incoming)) throw new IOException("Conflicting command for an existing Raft term/index");
                if (existing.isPresent() && existing.get().getTerm() != incoming.getTerm()) {
                    if (incoming.getIndex() <= commitIndex) throw new IOException("Cannot overwrite committed entry");
                    log.truncateFrom(incoming.getIndex()); existing = java.util.Optional.empty();
                }
                if (existing.isEmpty()) log.append(incoming.getTerm(), incoming.getCommand());
            }
            long updatedCommit = Math.min(leaderCommit, log.lastIndex());
            if (updatedCommit > commitIndex) {
                commitIndex = updatedCommit; metadata.save(term, votedFor, commitIndex); applyCommitted();
            }
            success = true;
        }
        long currentTerm = term; boolean result = success; long currentCommit = commitIndex;
        return response(request, RpcMessage.Type.APPEND_ENTRIES_RESPONSE, encode(out -> {
            out.writeLong(currentTerm); out.writeBoolean(result); out.writeLong(currentCommit);
        }));
    }

    private void notifyCommit() {
        for (Map.Entry<String, InetSocketAddress> peer : peers.entrySet())
            try { replicate(peer.getKey(), peer.getValue(), log.get(log.lastIndex()).orElse(null)); }
            catch (Exception ignored) { }
    }

    private void applyCommitted() throws IOException {
        while (lastApplied < commitIndex) {
            RaftLogEntry entry = log.get(++lastApplied).orElseThrow(() -> new IOException("Missing committed entry"));
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(entry.getCommand()));
            int op = in.readUnsignedByte();
            if (op == 0) continue;
            if (op == 7) { maintenanceFrozen = in.readUnsignedByte() != 0; continue; }
            if (op == 4) {
                int count = in.readInt();
                if (count < 0 || count > 1_000_000) throw new IOException("Invalid Raft batch count");
                Map<String, String> batch = new LinkedHashMap<>();
                for (int i = 0; i < count; i++) {
                    String key = in.readUTF(); int length = in.readInt();
                    if (length < 0 || length > MAX_COMMAND_BYTES) throw new IOException("Invalid batch value length");
                    byte[] value = in.readNBytes(length);
                    if (value.length != length) throw new IOException("Truncated batch value");
                    batch.put(key, new String(value, java.nio.charset.StandardCharsets.UTF_8));
                }
                store.setBatch(batch);
                continue;
            }
            String key = in.readUTF();
            if (op == 9) { store.del(key); continue; }
            if (op == 2) { store.del(key); continue; }
            long expiryAt = -1;
            if (op == 3 || op == 8) expiryAt = in.readLong();
            else if (op != 1) throw new IOException("Unknown state machine operation");
            int length = in.readInt();
            if (length < 0 || length > MAX_COMMAND_BYTES) throw new IOException("Invalid value length");
            byte[] value = in.readNBytes(length); if (value.length != length) throw new IOException("Truncated command");
            String decoded = new String(value, java.nio.charset.StandardCharsets.UTF_8);
            if (op == 3 || (op == 8 && expiryAt > 0)) store.setWithAbsoluteExpiry(key, decoded, expiryAt);
            else store.set(key, decoded);
        }
    }

    private void stepDown(long newTerm) throws IOException {
        if (newTerm > term) { term = newTerm; votedFor = null; metadata.save(term, null, commitIndex); }
        role = Role.FOLLOWER; leaderId = null;
        resetElectionDeadline();
    }
    private int majority() { return (peers.size() + 1) / 2 + 1; }
    private RpcMessage call(String target, InetSocketAddress address, RpcMessage.Type type, byte[] payload) throws IOException {
        byte[] scoped = encode(out -> { out.writeUTF(groupId); out.write(payload); });
        return new RpcTransportClient(address, 500, 1200).call(new RpcMessage(type, UUID.randomUUID().toString(), nodeId, target, scoped));
    }
    private RpcMessage response(RpcMessage req, RpcMessage.Type type, byte[] payload) {
        return new RpcMessage(type, req.getRequestId(), nodeId, req.getSourceNodeId(), payload);
    }
    private static DataInputStream input(RpcMessage m) { return new DataInputStream(new ByteArrayInputStream(m.getPayload())); }
    private interface Encoder { void write(DataOutputStream out) throws IOException; }
    private static byte[] encode(Encoder encoder) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) { encoder.write(out); }
        return bytes.toByteArray();
    }
    private static void writeEntry(DataOutputStream out, RaftLogEntry e) throws IOException {
        byte[] command = e.getCommand(); out.writeLong(e.getIndex()); out.writeLong(e.getTerm()); out.writeInt(command.length); out.write(command);
    }
    private static RaftLogEntry readEntry(DataInputStream in) throws IOException {
        long index = in.readLong(), term = in.readLong(); int length = in.readInt();
        if (length < 0 || length > MAX_COMMAND_BYTES) throw new IOException("Invalid command length");
        byte[] command = in.readNBytes(length); if (command.length != length) throw new IOException("Truncated entry");
        return new RaftLogEntry(index, term, command);
    }
    private static String require(String value, String field) { if (value == null || value.isBlank()) throw new IllegalArgumentException(field + " cannot be blank"); return value; }

    private void tickSafely() {
        try { tick(); }
        catch (Exception ignored) { /* Retry after the next randomized heartbeat/election interval. */ }
    }

    private synchronized void tick() throws IOException {
        if (!running) return;
        long now = System.nanoTime();
        if (role == Role.LEADER) {
            if (now - lastHeartbeatNanos >= TimeUnit.MILLISECONDS.toNanos(100)) {
                lastHeartbeatNanos = now;
                for (Map.Entry<String, InetSocketAddress> peer : peers.entrySet())
                    try { replicate(peer.getKey(), peer.getValue(), log.get(log.lastIndex()).orElse(null)); }
                    catch (IOException ignored) { }
            }
        } else if (now - lastContactNanos >= electionTimeoutNanos) startElection();
    }

    private void resetElectionDeadline() {
        lastContactNanos = System.nanoTime();
        electionTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(ThreadLocalRandom.current().nextLong(350, 650));
    }

    @Override public synchronized void close() throws IOException {
        running = false; scheduler.shutdownNow(); transport.close(); log.close();
    }
}
