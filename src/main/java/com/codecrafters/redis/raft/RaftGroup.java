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
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadLocalRandom;
import java.util.zip.CRC32;

/** One independently replicated Raft group hosted by a node. */
public final class RaftGroup implements AutoCloseable {
    private enum Role { FOLLOWER, CANDIDATE, LEADER }
    private static final int MAX_COMMAND_BYTES = 16 * 1024 * 1024;
    private static final int SNAPSHOT_CHUNK_BYTES = 8 * 1024 * 1024;
    private static final int MAX_SNAPSHOT_BYTES = 128 * 1024 * 1024;
    private static final int SNAPSHOT_LOG_ENTRY_THRESHOLD = positiveSetting("KV_RAFT_SNAPSHOT_LOG_ENTRIES", 10_000);
    private final String groupId, nodeId;
    private final Map<String, InetSocketAddress> peers;
    private final Set<String> voters = new java.util.LinkedHashSet<>();
    private Set<String> jointVoters;
    private Map<String, InetSocketAddress> pendingVoterPeers;
    private Set<String> uncommittedJointVoters;
    private Set<String> uncommittedFinalVoters;
    private final Map<String, Long> nextIndexByPeer = new java.util.HashMap<>();
    private final RaftLog log;
    private final RaftMetadataStore metadata;
    private final Path snapshotFile;
    private final InetSocketAddress bindAddress;
    private InetSocketAddress selfEndpoint;
    private final RedisStore store;
    private final RpcTransportServer transport;
    private final ScheduledExecutorService scheduler;
    private long term, commitIndex, lastApplied;
    private long lastContactNanos, electionTimeoutNanos, lastHeartbeatNanos;
    private final long electionTimeoutMinMillis = positiveSetting("KV_RAFT_ELECTION_MIN_MS", 350);
    private final long electionTimeoutMaxMillis = positiveSetting("KV_RAFT_ELECTION_MAX_MS", 650);
    private final int rpcConnectTimeoutMillis = positiveSetting("KV_RAFT_RPC_CONNECT_TIMEOUT_MS", 500);
    private final int rpcReadTimeoutMillis = positiveSetting("KV_RAFT_RPC_READ_TIMEOUT_MS", 1200);
    private String votedFor;
    private volatile String leaderId;
    private volatile Role role = Role.FOLLOWER;
    private boolean running;
    private boolean maintenanceFrozen;
    private long ownershipEpoch;
    private List<String> ownershipGroups = List.of();
    private List<String> migrationIntent = List.of();
    private final ThreadLocal<Long> requestOwnershipEpoch = new ThreadLocal<>();
    private final java.util.concurrent.atomic.AtomicBoolean migrationCoordinatorBusy = new java.util.concurrent.atomic.AtomicBoolean();
    private final Map<String, TransactionRecord> transactions = new LinkedHashMap<>();
    private final Map<String, String> preparedKeyOwners = new java.util.HashMap<>();
    private SnapshotTransfer snapshotTransfer;
    private record SnapshotTransfer(String id, String source, long index, long term,
                                    int totalBytes, int nextChunk, ByteArrayOutputStream bytes) { }

    /** Durable transaction metadata reconstructed from this group's committed Raft log. */
    public record TransactionRecord(String id, String digest, List<String> participants,
                                    Map<String, String> changes, Boolean decision,
                                    boolean prepared, boolean applied, long coordinatorTerm) {
        public TransactionRecord {
            participants = List.copyOf(participants);
            changes = java.util.Collections.unmodifiableMap(new LinkedHashMap<>(changes));
        }
    }

    public RaftGroup(String groupId, String nodeId, InetSocketAddress bindAddress,
                     Map<String, InetSocketAddress> peers, Path directory,
                     RedisStore store) throws IOException {
        this(groupId, nodeId, bindAddress, peers, directory, store, true);
    }

    /** Creates a non-voting learner which can be caught up before a joint voter change. */
    public RaftGroup(String groupId, String nodeId, InetSocketAddress bindAddress,
                     Map<String, InetSocketAddress> peers, Path directory,
                     RedisStore store, boolean initialVoter) throws IOException {
        this(groupId, nodeId, bindAddress, peers, directory, store, initialVoter, bindAddress);
    }

    /** Uses the advertised peer endpoint in snapshots when the listener binds a wildcard address. */
    public RaftGroup(String groupId, String nodeId, InetSocketAddress bindAddress,
                     Map<String, InetSocketAddress> peers, Path directory,
                     RedisStore store, boolean initialVoter,
                     InetSocketAddress advertisedAddress) throws IOException {
        this.groupId = require(groupId, "groupId");
        if (electionTimeoutMaxMillis <= electionTimeoutMinMillis)
            throw new IllegalArgumentException("KV_RAFT_ELECTION_MAX_MS must exceed KV_RAFT_ELECTION_MIN_MS");
        this.nodeId = require(nodeId, "nodeId");
        this.bindAddress = Objects.requireNonNull(bindAddress, "bindAddress");
        this.selfEndpoint = Objects.requireNonNull(advertisedAddress, "advertisedAddress");
        this.peers = new LinkedHashMap<>(Objects.requireNonNull(peers));
        if (this.peers.containsKey(nodeId)) throw new IllegalArgumentException("Peers must not contain this node");
        if (initialVoter) voters.add(nodeId);
        voters.addAll(this.peers.keySet());
        this.log = new RaftLog(directory.resolve("raft.log"));
        this.snapshotFile = directory.resolve("raft.snapshot");
        this.metadata = new RaftMetadataStore(directory.resolve("raft.meta"));
        RaftMetadataStore.State state = metadata.load();
        this.term = state.term(); this.votedFor = state.votedFor(); this.commitIndex = state.commitIndex();
        this.store = Objects.requireNonNull(store);
        if (Files.exists(snapshotFile)) {
            SnapshotHeader header = restoreSnapshot(Files.readAllBytes(snapshotFile));
            if (header.index() > log.lastIndex()) throw new IOException("Snapshot index exceeds recovered Raft log");
            log.installSnapshot(header.index(), header.term());
            commitIndex = Math.max(commitIndex, header.index());
            lastApplied = header.index();
            metadata.save(term, votedFor, commitIndex);
        }
        if (commitIndex > log.lastIndex()) throw new IOException("Persisted commit index exceeds the recovered log");
        applyCommitted();
        refreshPendingConfiguration();
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
    public String getGroupId() { return groupId; }
    public synchronized long getTerm() { return term; }
    public synchronized String getLeaderId() { return leaderId; }
    public synchronized boolean isLeader() { return role == Role.LEADER; }
    public synchronized long getCommitIndex() { return commitIndex; }
    public synchronized long getLogBaseIndex() { return log.baseIndex(); }
    public synchronized long getLastLogIndex() { return log.lastIndex(); }
    public synchronized long getLastLogTerm() { return log.lastTerm(); }
    public synchronized long getOwnershipEpoch() { return ownershipEpoch; }
    /** Local applied key count for monitoring; does not perform a quorum read. */
    public int getLocalStoreKeyCount() { return store.size(); }
    public synchronized List<String> getOwnershipGroups() { return ownershipGroups; }
    public synchronized List<String> getMigrationIntent() { return migrationIntent; }
    public synchronized boolean isMaintenanceFrozen() { return maintenanceFrozen; }
    public boolean tryAcquireMigrationCoordinator() { return migrationCoordinatorBusy.compareAndSet(false, true); }
    public boolean isMigrationCoordinatorBusy() { return migrationCoordinatorBusy.get(); }
    public void releaseMigrationCoordinator() { migrationCoordinatorBusy.set(false); }

    /** Stores the active cluster migration target in this group's replicated log. */
    public void setMigrationIntent(List<String> targetGroups) throws IOException {
        Objects.requireNonNull(targetGroups, "targetGroups");
        byte[] command = encode(out -> { out.writeByte(21); writeStrings(out, targetGroups); });
        commitCommand(command);
    }
    public synchronized Set<String> getVoterIds() { return Set.copyOf(voters); }

    /** Creates a durable checkpoint then compacts the committed prefix of the active Raft log. */
    public synchronized long createSnapshot() throws IOException {
        if (role != Role.LEADER) throw new IOException("Only the current leader may create a snapshot");
        long included = readIndex();
        while (lastApplied < included) applyCommitted();
        if (included <= log.baseIndex()) return log.baseIndex();
        long includedTerm = log.get(included).orElseThrow(() -> new IOException("Missing snapshot log boundary")).getTerm();
        persistSnapshot(encodeSnapshot(included, includedTerm));
        log.compactThrough(included, includedTerm);
        metadata.save(term, votedFor, Math.max(commitIndex, included));
        return included;
    }

    @FunctionalInterface public interface IoAction<T> { T run() throws IOException; }

    /** Runs a routed operation with its cached ownership epoch attached to the Raft command. */
    public <T> T withOwnershipEpoch(long expectedEpoch, IoAction<T> action) throws IOException {
        Long previous = requestOwnershipEpoch.get();
        requestOwnershipEpoch.set(expectedEpoch);
        try { return action.run(); }
        finally { if (previous == null) requestOwnershipEpoch.remove(); else requestOwnershipEpoch.set(previous); }
    }

    /** Advances this group's durable ownership fence by exactly one. */
    public long advanceOwnershipEpoch(long nextEpoch) throws IOException {
        return advanceOwnershipEpoch(nextEpoch, ownershipGroups);
    }

    public long advanceOwnershipEpoch(long nextEpoch, List<String> activeGroups) throws IOException {
        if (activeGroups == null || activeGroups.isEmpty()) throw new IllegalArgumentException("At least one owner group is required");
        byte[] command = encode(out -> { out.writeByte(20); out.writeLong(nextEpoch); writeStrings(out, activeGroups); });
        synchronized (this) {
            if (role == Role.LEADER && nextEpoch == ownershipEpoch
                    && ownershipGroups.equals(activeGroups)) return commitIndex;
            if (role == Role.LEADER && nextEpoch != ownershipEpoch + 1)
                throw new IOException("Ownership epoch must increase by exactly one");
        }
        long committed = commitCommand(command);
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            synchronized (this) {
                if (ownershipEpoch == nextEpoch && ownershipGroups.equals(activeGroups)) return committed;
                if (ownershipEpoch > nextEpoch) throw new IOException("Ownership epoch advanced beyond requested migration epoch");
            }
            try { Thread.sleep(10); }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted waiting for committed ownership epoch to apply", e);
            }
        }
        throw new IOException("Committed ownership epoch was not applied locally before timeout");
    }

    /**
     * Change the voter set using Raft joint consensus. The supplied map includes
     * every target voter and must already be reachable.
     * New voters are caught up by log replication before the joint entry commits.
     */
    public synchronized void reconfigureVoters(Map<String, InetSocketAddress> target) throws IOException {
        Objects.requireNonNull(target, "target");
        if (role != Role.LEADER) {
            String knownLeader = leaderId;
            InetSocketAddress address = knownLeader == null ? null : peers.get(knownLeader);
            if (address == null) throw new IOException("Current Raft leader is unknown");
            byte[] command = encode(out -> {
                out.writeByte(23); out.writeInt(target.size());
                for (Map.Entry<String, InetSocketAddress> voter : target.entrySet()) {
                    out.writeUTF(voter.getKey()); out.writeUTF(voter.getValue().getHostString()); out.writeInt(voter.getValue().getPort());
                }
            });
            RpcMessage response = call(knownLeader, address, RpcMessage.Type.REQUEST, command);
            DataInputStream result = input(response);
            if (!result.readBoolean()) { result.readLong(); result.readBoolean(); result.readInt(); throw new IOException(result.readUTF()); }
            return;
        }
        if (target.isEmpty()) throw new IllegalArgumentException("Target voter map cannot be empty");
        Set<String> nextVoters = new java.util.LinkedHashSet<>(target.keySet());
        if (target.containsKey(nodeId)) selfEndpoint = target.get(nodeId);
        if (nextVoters.equals(voters) && jointVoters == null) return;
        Set<String> activeJoint = uncommittedJointVoters != null ? uncommittedJointVoters : jointVoters;
        if (activeJoint != null && !activeJoint.equals(nextVoters))
            throw new IOException("A different Raft voter transition is already joint; finish it first");
        if (uncommittedJointVoters != null && jointVoters == null)
            commitAsLeader(new byte[] {0}); // current-term barrier commits the joint entry under both quorums
        if (uncommittedFinalVoters != null) {
            commitAsLeader(new byte[] {0}); // retry a final configuration left uncommitted by a prior quorum loss
            return;
        }
        if (activeJoint == null) {
            for (Map.Entry<String, InetSocketAddress> peer : target.entrySet()) {
                if (!peer.getKey().equals(nodeId)) peers.put(peer.getKey(), peer.getValue());
            }
            byte[] joint = encode(out -> {
                out.writeByte(18); out.writeInt(target.size());
                for (Map.Entry<String, InetSocketAddress> voter : target.entrySet()) {
                    out.writeUTF(voter.getKey()); out.writeUTF(voter.getValue().getHostString());
                    out.writeInt(voter.getValue().getPort());
                }
            });
            pendingVoterPeers = new LinkedHashMap<>(target);
            commitAsLeader(joint);
        }
        commitAsLeader(new byte[] {19});
    }

    /** Starts one election round. A majority vote is required to become leader. */
    public boolean startElection() throws IOException {
        final long electionTerm, lastIndex, lastTerm;
        final Map<String, InetSocketAddress> electionPeers;
        synchronized (this) {
            if (!isEligibleVoter(nodeId)) {
                role = Role.FOLLOWER; leaderId = null; return false;
            }
            role = Role.CANDIDATE; leaderId = null; term++; votedFor = nodeId;
            resetElectionDeadline(); metadata.save(term, votedFor, commitIndex);
            electionTerm = term; lastIndex = log.lastIndex(); lastTerm = log.lastTerm();
            electionPeers = new LinkedHashMap<>(peers);
        }
        Set<String> votes = new java.util.HashSet<>(); votes.add(nodeId);
        long highestTerm = electionTerm;
        for (Map.Entry<String, InetSocketAddress> peer : electionPeers.entrySet()) {
            try {
                RpcMessage response = call(peer.getKey(), peer.getValue(), RpcMessage.Type.VOTE_REQUEST,
                        encode(out -> { out.writeLong(electionTerm); out.writeLong(lastIndex); out.writeLong(lastTerm); }));
                if (response.getType() != RpcMessage.Type.VOTE_RESPONSE) continue;
                DataInputStream in = input(response); long responseTerm = in.readLong(); boolean granted = in.readBoolean();
                highestTerm = Math.max(highestTerm, responseTerm);
                if (responseTerm == electionTerm && granted) votes.add(peer.getKey());
            } catch (IOException failure) {
                if (Boolean.getBoolean("kv.raft.debug"))
                    System.err.println("Raft " + nodeId + " vote RPC to " + peer.getKey() + " failed: " + failure);
            }
        }
        final RaftLogEntry noOp;
        synchronized (this) {
            if (highestTerm > term) { stepDown(highestTerm); return false; }
            if (term != electionTerm || role != Role.CANDIDATE) return false;
            if (Boolean.getBoolean("kv.raft.debug"))
                System.err.println("Raft " + nodeId + " election term=" + electionTerm + " votes=" + votes + " voters=" + voters);
            if (!hasQuorum(votes)) {
                role = Role.FOLLOWER; resetElectionDeadline(); return false;
            }
            role = Role.LEADER; leaderId = nodeId;
            synchronized (nextIndexByPeer) {
                nextIndexByPeer.clear();
                for (String peerId : peers.keySet()) nextIndexByPeer.put(peerId, 1L);
            }
            noOp = log.append(term, new byte[] {0});
        }
        Set<String> replicated = new java.util.HashSet<>(); replicated.add(nodeId);
        for (Map.Entry<String, InetSocketAddress> peer : electionPeers.entrySet()) {
            try { if (replicate(peer.getKey(), peer.getValue(), noOp)) replicated.add(peer.getKey()); }
            catch (IOException failure) {
                if (Boolean.getBoolean("kv.raft.debug"))
                    System.err.println("Raft " + nodeId + " election append to " + peer.getKey() + " failed: " + failure);
            }
            synchronized (this) { if (term != electionTerm || role != Role.LEADER) return false; }
        }
        synchronized (this) {
            if (term != electionTerm || role != Role.LEADER) return false;
            if (Boolean.getBoolean("kv.raft.debug"))
                System.err.println("Raft " + nodeId + " no-op replication term=" + electionTerm + " replicas=" + replicated);
            if (!hasQuorum(replicated)) { role = Role.FOLLOWER; leaderId = null; resetElectionDeadline(); return false; }
            commitIndex = noOp.getIndex(); metadata.save(term, votedFor, commitIndex); applyCommitted();
            refreshPendingConfiguration();
            notifyCommit(); maybeCreateSnapshot(); lastHeartbeatNanos = System.nanoTime();
            return true;
        }
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

    /** Commits a multi-key delete as one Raft log entry and one storage transaction. */
    public int deleteBatch(String... keys) throws IOException {
        ensureWritesAllowed();
        Objects.requireNonNull(keys, "keys");
        if (keys.length == 0) return 0;
        String[] uniqueKeys = new java.util.LinkedHashSet<>(java.util.Arrays.asList(keys)).toArray(String[]::new);
        byte[] command = encode(out -> {
            out.writeByte(10); out.writeInt(uniqueKeys.length);
            for (String key : uniqueKeys) {
                if (key == null) throw new IllegalArgumentException("Delete key cannot be null");
                out.writeUTF(key);
            }
        });
        synchronized (this) {
            if (role == Role.LEADER) {
                int existed = 0;
                for (String key : uniqueKeys) if (store.exists(key)) existed++;
                commitAsLeader(command);
                return existed;
            }
        }
        return forwardClientCommand(command).deleteCount();
    }

    public void beginTransaction(String id, String digest, List<String> participants) throws IOException {
        byte[] command = encode(out -> {
            out.writeByte(11); out.writeUTF(id); out.writeUTF(digest); writeStrings(out, participants);
        });
        commitCommand(command);
    }

    public void prepareTransaction(String id, String digest, List<String> participants,
                                   Map<String, String> changes) throws IOException {
        byte[] command = encode(out -> {
            out.writeByte(12); out.writeUTF(id); out.writeUTF(digest); writeStrings(out, participants);
            out.writeInt(changes.size());
            for (Map.Entry<String, String> change : changes.entrySet()) {
                out.writeUTF(change.getKey()); out.writeBoolean(change.getValue() != null);
                if (change.getValue() != null) out.writeUTF(change.getValue());
            }
        });
        if (command.length > MAX_COMMAND_BYTES) throw new IOException("Transaction prepare exceeds Raft command limit");
        synchronized (this) { validatePrepareAvailable(id, changes.keySet()); }
        commitCommand(command);
    }

    public void decideTransaction(String id, String digest, List<String> participants, boolean commit)
            throws IOException {
        byte[] command = encode(out -> {
            out.writeByte(13); out.writeUTF(id); out.writeUTF(digest); writeStrings(out, participants);
            out.writeBoolean(commit);
        });
        commitCommand(command);
    }

    public void applyCommittedTransaction(String id, String digest) throws IOException {
        byte[] command = encode(out -> { out.writeByte(14); out.writeUTF(id); out.writeUTF(digest); });
        commitCommand(command);
    }

    public void abortPreparedTransaction(String id, String digest) throws IOException {
        byte[] command = encode(out -> { out.writeByte(15); out.writeUTF(id); out.writeUTF(digest); });
        commitCommand(command);
    }

    /** Linearizable transaction state, forwarded to the group leader when called on a follower. */
    public List<TransactionRecord> transactionsWithReadIndex() throws IOException {
        synchronized (this) {
            if (role == Role.LEADER) {
                // The caller first quorum-commits the freeze command. That committed
                // entry is the snapshot barrier; doing ReadIndex here would make an
                // RPC worker synchronously wait for nested peer RPCs.
                long index = commitIndex;
                while (lastApplied < index) applyCommitted();
                return List.copyOf(transactions.values());
            }
        }
        DataInputStream in = forwardClientRead(new byte[] {16});
        int count = in.readInt();
        if (count < 0 || count > 1_000_000) throw new IOException("Invalid transaction state count");
        List<TransactionRecord> result = new java.util.ArrayList<>(count);
        for (int i = 0; i < count; i++) result.add(readTransactionRecord(in));
        return List.copyOf(result);
    }

    public synchronized boolean hasUnresolvedLocalTransactions() {
        for (TransactionRecord record : transactions.values())
            if (record.decision() == null || record.prepared() || (Boolean.TRUE.equals(record.decision()) && !record.applied()))
                return true;
        return false;
    }

    public long currentTermWithReadIndex() throws IOException {
        synchronized (this) {
            if (role == Role.LEADER) { readIndex(); return term; }
        }
        return forwardClientRead(new byte[] {17}).readLong();
    }

    private long commitCommand(byte[] command) throws IOException {
        Long expected = requestOwnershipEpoch.get();
        return expected == null ? commitCommand(command, -1) : commitCommand(command, expected);
    }

    private long commitCommand(byte[] command, long expectedEpoch) throws IOException {
        synchronized (this) {
            if (expectedEpoch >= 0 && expectedEpoch != ownershipEpoch)
                throw new IOException("Stale shard owner: request epoch=" + expectedEpoch + ", current epoch=" + ownershipEpoch);
            int op = command.length == 0 ? -1 : command[0] & 0xff;
            if (maintenanceFrozen && op != 7 && op != 8 && op != 9 && op != 13 && op != 14 && op != 15 && op != 20 && op != 21)
                throw new IOException("Raft group is frozen for shard migration");
            if (role == Role.LEADER) validateOwnershipCommand(command);
            validateNoPreparedConflict(command);
            if (role == Role.LEADER) {
                validateTransactionCommand(command);
                Long previous = requestOwnershipEpoch.get();
                if (expectedEpoch >= 0) requestOwnershipEpoch.set(expectedEpoch);
                try { return commitAsLeader(command); }
                finally { if (previous == null) requestOwnershipEpoch.remove(); else requestOwnershipEpoch.set(previous); }
            }
        }
        if (expectedEpoch >= 0) {
            Long previous = requestOwnershipEpoch.get(); requestOwnershipEpoch.set(expectedEpoch);
            try { return forwardClientCommand(command).index(); }
            finally { if (previous == null) requestOwnershipEpoch.remove(); else requestOwnershipEpoch.set(previous); }
        }
        return forwardClientCommand(command).index();
    }

    private synchronized void ensureWritesAllowed() throws IOException {
        if (maintenanceFrozen) throw new IOException("Raft group is frozen for shard migration");
    }

    /** Quorum-committed write fence used while a Multi-Raft shard migration is in progress. */
    public long setMaintenanceFrozen(boolean frozen) throws IOException {
        byte[] command = new byte[] {7, (byte) (frozen ? 1 : 0)};
        return commitCommand(command);
    }

    /** Returns an expiry-preserving view of all live values after a linearizable read fence. */
    public Map<String, ValueEntry> snapshotEntriesWithReadIndex() throws IOException {
        boolean leader;
        synchronized (this) { leader = role == Role.LEADER; }
        if (!leader) {
            DataInputStream result = forwardClientRead(new byte[] {24});
            int count = result.readInt();
            if (count < 0 || count > 1_000_000) throw new IOException("Invalid migration snapshot count");
            Map<String, ValueEntry> entries = new LinkedHashMap<>();
            for (int i = 0; i < count; i++) {
                String key = result.readUTF(); long expiry = result.readLong(); int length = result.readInt();
                if (length < 0 || length > MAX_COMMAND_BYTES) throw new IOException("Invalid migration snapshot value");
                byte[] bytes = result.readNBytes(length);
                if (bytes.length != length) throw new IOException("Truncated migration snapshot value");
                entries.put(key, new ValueEntry(new String(bytes, java.nio.charset.StandardCharsets.UTF_8), expiry));
            }
            return Map.copyOf(entries);
        }
        synchronized (this) {
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
        return commitCommand(command);
    }

    private long commitAsLeader(byte[] command) throws IOException {
        Long expectedEpoch = requestOwnershipEpoch.get();
        if (ownershipEpoch > 0 && expectedEpoch == null && requiresOwnerFence(command))
            throw new IOException("Ownership epoch token is required for this shard");
        if (expectedEpoch != null && expectedEpoch != ownershipEpoch)
            throw new IOException("Stale shard owner: request epoch=" + expectedEpoch + ", current epoch=" + ownershipEpoch);
        long writeTerm = term;
        RaftLogEntry entry = log.append(term, command);
        refreshPendingConfiguration();
        Set<String> replicated = new java.util.HashSet<>(); replicated.add(nodeId);
        for (Map.Entry<String, InetSocketAddress> peer : peers.entrySet()) {
            try { if (replicate(peer.getKey(), peer.getValue(), entry)) replicated.add(peer.getKey()); }
            catch (IOException ignored) { }
            if (term != writeTerm || role != Role.LEADER) throw new IOException("Leadership changed while replicating write");
        }
        if (!hasQuorumForCommand(replicated, command)) throw new IOException("Raft write did not reach the required voter quorum");
        commitIndex = entry.getIndex(); metadata.save(term, votedFor, commitIndex);
        // Notify the removed voters while the joint configuration still retains
        // their endpoints, so they learn the final entry is committed before
        // applying it removes those endpoints locally.
        if (command.length > 0 && command[0] == 19) notifyCommit();
        applyCommitted();
        refreshPendingConfiguration();
        notifyCommit();
        maybeCreateSnapshot();
        return commitIndex;
    }

    private void maybeCreateSnapshot() {
        if (role == Role.LEADER && log.size() >= SNAPSHOT_LOG_ENTRY_THRESHOLD) {
            try { createSnapshot(); }
            catch (IOException ignored) { /* Keep the complete log if checkpointing fails. */ }
        }
    }

    private ClientResult forwardClientCommand(byte[] command) throws IOException {
        String currentLeader = leaderId;
        InetSocketAddress address = currentLeader == null ? null : peers.get(currentLeader);
        if (address == null) throw new IOException("Leader is not known yet");
        Long expectedEpoch = requestOwnershipEpoch.get();
        byte[] forwarded = expectedEpoch == null ? command : encode(out -> {
            out.writeByte(22); out.writeLong(expectedEpoch); out.writeInt(command.length); out.write(command);
        });
        RpcMessage response = call(currentLeader, address, RpcMessage.Type.REQUEST, forwarded);
        if (response.getType() != RpcMessage.Type.RESPONSE) throw new IOException("Invalid Raft client response");
        DataInputStream in = input(response);
        boolean success = in.readBoolean(); long index = in.readLong(); boolean keyExisted = in.readBoolean();
        int deleteCount = in.readInt();
        if (!success) throw new IOException(in.readUTF());
        return new ClientResult(index, keyExisted, deleteCount);
    }

    private DataInputStream forwardClientRead(byte[] command) throws IOException {
        String currentLeader = leaderId;
        InetSocketAddress address = currentLeader == null ? null : peers.get(currentLeader);
        if (address == null) throw new IOException("Leader is not known yet");
        RpcMessage response = call(currentLeader, address, RpcMessage.Type.REQUEST, command);
        if (response.getType() != RpcMessage.Type.RESPONSE) throw new IOException("Invalid Raft read response");
        DataInputStream in = input(response);
        boolean success = in.readBoolean(); in.readLong(); in.readBoolean();
        in.readInt();
        if (!success) throw new IOException(in.readUTF());
        return in;
    }

    private synchronized RpcMessage handleClientCommand(RpcMessage request) throws IOException {
        DataInputStream in = input(request);
        byte[] command = in.readAllBytes();
        Long expectedEpoch = null;
        if (command.length > 0 && command[0] == 22) {
            DataInputStream envelope = new DataInputStream(new ByteArrayInputStream(command));
            envelope.readUnsignedByte(); expectedEpoch = envelope.readLong();
            int length = envelope.readInt();
            if (length < 1 || length > MAX_COMMAND_BYTES || envelope.available() != length)
                throw new IOException("Malformed fenced Raft command");
            command = envelope.readNBytes(length);
            if (expectedEpoch != ownershipEpoch) {
                long requested = expectedEpoch, current = ownershipEpoch;
                return response(request, RpcMessage.Type.RESPONSE, encode(out -> {
                    out.writeBoolean(false); out.writeLong(-1); out.writeBoolean(false); out.writeInt(0);
                    out.writeUTF("Stale shard owner: request epoch=" + requested + ", current epoch=" + current);
                }));
            }
        }
        if (command.length > 0 && command[0] == 25) {
            try {
                installSnapshotChunk(request.getSourceNodeId(), command);
                return response(request, RpcMessage.Type.RESPONSE,
                        encode(out -> { out.writeBoolean(true); out.writeLong(commitIndex); out.writeBoolean(false); out.writeInt(0); }));
            } catch (IOException failure) {
                String reason = failure.getMessage() == null ? "Snapshot installation failed" : failure.getMessage();
                return response(request, RpcMessage.Type.RESPONSE,
                        encode(out -> { out.writeBoolean(false); out.writeLong(-1); out.writeBoolean(false); out.writeInt(0); out.writeUTF(reason); }));
            }
        }
        if (command.length > 0 && command[0] == 23) {
            try {
                if (role != Role.LEADER) throw new IOException("Not leader; leader=" + leaderId);
                DataInputStream config = new DataInputStream(new ByteArrayInputStream(command));
                config.readUnsignedByte(); Map<String, InetSocketAddress> target = readVoterConfig(config);
                if (config.available() != 0) throw new IOException("Trailing bytes in voter configuration request");
                reconfigureVoters(target);
                return response(request, RpcMessage.Type.RESPONSE,
                        encode(out -> { out.writeBoolean(true); out.writeLong(commitIndex); out.writeBoolean(false); out.writeInt(0); }));
            } catch (IOException | RuntimeException failure) {
                String reason = failure.getMessage() == null ? "Voter reconfiguration failed" : failure.getMessage();
                return response(request, RpcMessage.Type.RESPONSE,
                        encode(out -> { out.writeBoolean(false); out.writeLong(-1); out.writeBoolean(false); out.writeInt(0); out.writeUTF(reason); }));
            }
        }
        if (command.length == 1 && command[0] == 24) {
            try {
                if (role != Role.LEADER) throw new IOException("Not leader; leader=" + leaderId);
                if (!maintenanceFrozen) throw new IOException("Migration snapshot requires a frozen Raft group");
                long index = commitIndex;
                while (lastApplied < index) applyCommitted();
                Map<String, ValueEntry> entries = new LinkedHashMap<>();
                for (String key : store.keys("*")) {
                    ValueEntry entry = store.getEntry(key);
                    if (entry != null && !entry.isExpired()) entries.put(key, entry);
                }
                return response(request, RpcMessage.Type.RESPONSE, encode(out -> {
                    out.writeBoolean(true); out.writeLong(index); out.writeBoolean(false); out.writeInt(0);
                    out.writeInt(entries.size());
                    for (Map.Entry<String, ValueEntry> entry : entries.entrySet()) {
                        byte[] value = entry.getValue().getValue().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                        out.writeUTF(entry.getKey()); out.writeLong(entry.getValue().getExpiryTimestampMillis());
                        out.writeInt(value.length); out.write(value);
                    }
                }));
            } catch (IOException failure) {
                String reason = failure.getMessage() == null ? "Migration snapshot failed" : failure.getMessage();
                return response(request, RpcMessage.Type.RESPONSE,
                        encode(out -> { out.writeBoolean(false); out.writeLong(-1); out.writeBoolean(false); out.writeInt(0); out.writeUTF(reason); }));
            }
        }
        if (command.length > 0 && command[0] == 16) {
            try {
                if (command.length != 1) throw new IOException("Invalid transaction read command");
                if (role != Role.LEADER) throw new IOException("Not leader; leader=" + leaderId);
                long index = readIndex();
                while (lastApplied < index) applyCommitted();
                List<TransactionRecord> records = List.copyOf(transactions.values());
                return response(request, RpcMessage.Type.RESPONSE, encode(out -> {
                    out.writeBoolean(true); out.writeLong(index); out.writeBoolean(false); out.writeInt(0);
                    out.writeInt(records.size());
                    for (TransactionRecord record : records) writeTransactionRecord(out, record);
                }));
            } catch (IOException failure) {
                String reason = failure.getMessage() == null ? "Transaction read failed" : failure.getMessage();
                return response(request, RpcMessage.Type.RESPONSE, encode(out -> {
                    out.writeBoolean(false); out.writeLong(-1); out.writeBoolean(false); out.writeInt(0);
                    out.writeUTF(reason);
                }));
            }
        }
        if (command.length == 1 && command[0] == 17) {
            try {
                if (role != Role.LEADER) throw new IOException("Not leader; leader=" + leaderId);
                long index = readIndex(); long currentTerm = term;
                return response(request, RpcMessage.Type.RESPONSE, encode(out -> {
                    out.writeBoolean(true); out.writeLong(index); out.writeBoolean(false); out.writeInt(0);
                    out.writeLong(currentTerm);
                }));
            } catch (IOException failure) {
                String reason = failure.getMessage() == null ? "Term read failed" : failure.getMessage();
                return response(request, RpcMessage.Type.RESPONSE, encode(out -> {
                    out.writeBoolean(false); out.writeLong(-1); out.writeBoolean(false); out.writeInt(0); out.writeUTF(reason);
                }));
            }
        }
        if (command.length > 0 && (command[0] == 5 || command[0] == 6)) {
            try {
                if (role != Role.LEADER) throw new IOException("Not leader; leader=" + leaderId);
                DataInputStream read = new DataInputStream(new ByteArrayInputStream(command));
                int operation = read.readUnsignedByte(); String argument = read.readUTF();
                if (read.available() != 0) throw new IOException("Trailing bytes in forwarded read");
                long index = readIndex();
                if (operation == 5) {
                    ensureKeyNotPrepared(argument);
                    byte[] value = get(argument);
                    return response(request, RpcMessage.Type.RESPONSE, encode(out -> {
                        out.writeBoolean(true); out.writeLong(index); out.writeBoolean(false); out.writeInt(0);
                        out.writeInt(value == null ? -1 : value.length);
                        if (value != null) out.write(value);
                    }));
                }
                List<String> keys = store.keys(argument);
                return response(request, RpcMessage.Type.RESPONSE, encode(out -> {
                    out.writeBoolean(true); out.writeLong(index); out.writeBoolean(false); out.writeInt(0);
                    out.writeInt(keys.size()); for (String key : keys) out.writeUTF(key);
                }));
            } catch (IOException failure) {
                String reason = failure.getMessage() == null ? "Raft read failed" : failure.getMessage();
                return response(request, RpcMessage.Type.RESPONSE,
                        encode(out -> { out.writeBoolean(false); out.writeLong(-1); out.writeBoolean(false); out.writeInt(0); out.writeUTF(reason); }));
            }
        }
        try { validateWriteCommand(command); }
        catch (IOException invalid) {
            String reason = invalid.getMessage() == null ? "Invalid Raft command" : invalid.getMessage();
            return response(request, RpcMessage.Type.RESPONSE,
                    encode(out -> { out.writeBoolean(false); out.writeLong(-1); out.writeBoolean(false); out.writeInt(0); out.writeUTF(reason); }));
        }
        boolean keyExisted = false;
        int deleteCount = 0;
        if (role == Role.LEADER && command.length > 0 && command[0] == 2) {
            DataInputStream deleteCommand = new DataInputStream(new ByteArrayInputStream(command));
            deleteCommand.readUnsignedByte(); keyExisted = store.exists(deleteCommand.readUTF());
            deleteCount = keyExisted ? 1 : 0;
        } else if (role == Role.LEADER && command.length > 0 && command[0] == 10) {
            DataInputStream deleteCommand = new DataInputStream(new ByteArrayInputStream(command));
            deleteCommand.readUnsignedByte(); int count = deleteCommand.readInt();
            for (int i = 0; i < count; i++) if (store.exists(deleteCommand.readUTF())) deleteCount++;
        }
        try {
            if (role != Role.LEADER) throw new IOException("Not leader; leader=" + leaderId);
            int op = command.length == 0 ? -1 : command[0] & 0xff;
            if (maintenanceFrozen && op != 7 && op != 8 && op != 9 && op != 13 && op != 14 && op != 15 && op != 20 && op != 21)
                throw new IOException("Raft group is frozen for shard migration");
            long index = expectedEpoch == null ? commitCommand(command) : commitCommand(command, expectedEpoch);
            boolean existed = keyExisted;
            int deleted = deleteCount;
            return response(request, RpcMessage.Type.RESPONSE,
                    encode(out -> { out.writeBoolean(true); out.writeLong(index); out.writeBoolean(existed); out.writeInt(deleted); }));
        } catch (IOException failure) {
            String reason = failure.getMessage() == null ? "Raft request failed" : failure.getMessage();
            return response(request, RpcMessage.Type.RESPONSE,
                    encode(out -> { out.writeBoolean(false); out.writeLong(-1); out.writeBoolean(false); out.writeInt(0); out.writeUTF(reason); }));
        }
    }

    private record ClientResult(long index, boolean keyExisted, int deleteCount) { }

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
        else if (operation == 10) {
            int count = in.readInt();
            if (count < 0 || count > 1_000_000) throw new IOException("Invalid delete batch count");
            for (int i = 0; i < count; i++) in.readUTF();
        } else if (operation == 11) {
            in.readUTF(); in.readUTF(); readStrings(in);
        } else if (operation == 12) {
            in.readUTF(); in.readUTF(); readStrings(in);
            int count = in.readInt();
            if (count < 0 || count > 1_000_000) throw new IOException("Invalid prepared change count");
            for (int i = 0; i < count; i++) { in.readUTF(); if (in.readBoolean()) in.readUTF(); }
        } else if (operation == 13) {
            in.readUTF(); in.readUTF(); readStrings(in); in.readBoolean();
        } else if (operation == 14 || operation == 15) {
            in.readUTF(); in.readUTF();
        } else if (operation == 20) {
            if (in.readLong() <= 0) throw new IOException("Invalid ownership epoch");
            List<String> groups = readStrings(in);
            if (groups.isEmpty()) throw new IOException("Ownership manifest cannot be empty");
        } else if (operation == 21) {
            List<String> groups = readStrings(in);
            if (groups.size() > 100_000 || new java.util.LinkedHashSet<>(groups).size() != groups.size())
                throw new IOException("Invalid migration target group list");
        } else throw new IOException("Unknown state machine operation");
        if (in.available() != 0) throw new IOException("Trailing bytes in Raft command");
    }

    private synchronized void validateOwnershipCommand(byte[] command) throws IOException {
        if (command.length == 0 || command[0] != 20) return;
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(command));
        in.readUnsignedByte(); long next = in.readLong(); readStrings(in);
        if (next != ownershipEpoch + 1)
            throw new IOException("Ownership epoch must increase by exactly one; current=" + ownershipEpoch);
    }

    public synchronized byte[] get(String key) {
        String value = store.get(key);
        return value == null ? null : value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    /** Linearizable read fence: a majority must confirm this leader's current term. */
    public synchronized long readIndex() throws IOException {
        if (role != Role.LEADER) throw new IOException("Not leader; leader=" + leaderId);
        long readTerm = term; Set<String> confirmations = new java.util.HashSet<>(); confirmations.add(nodeId);
        for (Map.Entry<String, InetSocketAddress> peer : peers.entrySet()) {
            try { if (replicate(peer.getKey(), peer.getValue(), log.get(log.lastIndex()).orElse(null))) confirmations.add(peer.getKey()); }
            catch (IOException ignored) { }
        }
        if (role != Role.LEADER || term != readTerm || !hasQuorum(confirmations))
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
        verifyRequestOwnershipEpoch();
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
        verifyRequestOwnershipEpoch();
        if (role != Role.LEADER) {
            DataInputStream result = forwardClientRead(encode(out -> { out.writeByte(5); out.writeUTF(key); }));
            int length = result.readInt();
            if (length < 0) return null;
            if (length > MAX_COMMAND_BYTES) throw new IOException("Invalid forwarded read length");
            byte[] value = result.readNBytes(length);
            if (value.length != length) throw new IOException("Truncated forwarded read");
            return value;
        }
        ensureKeyNotPrepared(key);
        long index = readIndex();
        while (lastApplied < index) applyCommitted();
        return get(key);
    }

    public List<String> keysWithReadIndex(String pattern) throws IOException {
        verifyRequestOwnershipEpoch();
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
        verifyRequestOwnershipEpoch();
        getBoundedStalenessRead("", maxCommitLag); // perform the same freshness and authority check
        synchronized (this) { return store.keys(pattern); }
    }

    private boolean replicate(String peerId, InetSocketAddress address, RaftLogEntry entry) throws IOException {
        long throughIndex = entry == null ? 0 : entry.getIndex();
        long next;
        synchronized (nextIndexByPeer) { next = nextIndexByPeer.getOrDefault(peerId, 1L); }
        if (next <= log.baseIndex()) {
            if (!Files.exists(snapshotFile)) createSnapshot();
            sendSnapshot(peerId, address);
            next = log.baseIndex() + 1;
            synchronized (nextIndexByPeer) { nextIndexByPeer.put(peerId, next); }
        }
        int retries = 0;
        while (next <= throughIndex) {
            if (next <= log.baseIndex()) {
                if (!Files.exists(snapshotFile)) createSnapshot();
                sendSnapshot(peerId, address);
                next = log.baseIndex() + 1;
                synchronized (nextIndexByPeer) { nextIndexByPeer.put(peerId, next); }
                retries = 0;
            }
            long requestedIndex = next;
            RaftLogEntry item = log.get(requestedIndex)
                    .orElseThrow(() -> new IOException("Leader log is missing index " + requestedIndex));
            long previousIndex = item.getIndex() - 1;
            long previousTerm = previousIndex == log.baseIndex() ? log.baseTerm()
                    : log.get(previousIndex).map(RaftLogEntry::getTerm).orElse(0L);
            byte[] payload = encode(out -> {
                out.writeLong(term); out.writeLong(previousIndex);
                out.writeLong(previousTerm); out.writeLong(commitIndex); out.writeBoolean(true); writeEntry(out, item);
            });
            RpcMessage response = call(peerId, address, RpcMessage.Type.APPEND_ENTRIES_REQUEST, payload);
            if (response.getType() != RpcMessage.Type.APPEND_ENTRIES_RESPONSE) return false;
            DataInputStream in = input(response); long responseTerm = in.readLong(); boolean success = in.readBoolean();
            long followerCommit = in.readLong();
            if (responseTerm > term) { stepDown(responseTerm); return false; }
            if (!success) {
                if (followerCommit > log.lastIndex()) return false;
                next = Math.max(1, Math.max(next - 1, followerCommit + 1));
                synchronized (nextIndexByPeer) { nextIndexByPeer.put(peerId, next); }
                if (++retries > throughIndex + 1) return false;
                continue;
            }
            next = item.getIndex() + 1;
            synchronized (nextIndexByPeer) { nextIndexByPeer.put(peerId, next); }
            retries = 0;
        }
        long previousTerm = throughIndex == log.baseIndex() ? log.baseTerm()
                : log.get(throughIndex).map(RaftLogEntry::getTerm).orElse(0L);
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

    private record SnapshotHeader(long index, long term) { }
    private record SnapshotData(SnapshotHeader header, Map<String, ValueEntry> entries, boolean frozen,
                                long ownershipEpoch, List<String> ownershipGroups,
                                Map<String, InetSocketAddress> voters,
                                Map<String, InetSocketAddress> jointVoters,
                                Map<String, InetSocketAddress> pendingVoters,
                                List<TransactionRecord> transactions, List<String> migrationIntent) { }

    private byte[] encodeSnapshot(long index, long includedTerm) throws IOException {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(payload)) {
            out.writeInt(0x4b565331); out.writeLong(index); out.writeLong(includedTerm);
            out.writeBoolean(maintenanceFrozen); out.writeLong(ownershipEpoch);
            writeStringList(out, ownershipGroups);
            writeEndpointMap(out, voterEndpoints(voters, peers));
            out.writeBoolean(jointVoters != null);
            if (jointVoters != null) writeEndpointMap(out, voterEndpoints(jointVoters, pendingVoterPeers == null ? peers : pendingVoterPeers));
            out.writeBoolean(pendingVoterPeers != null);
            if (pendingVoterPeers != null) writeEndpointMap(out, pendingVoterPeers);
            Map<String, ValueEntry> entries = store.snapshotEntries();
            out.writeInt(entries.size());
            for (Map.Entry<String, ValueEntry> row : entries.entrySet()) {
                out.writeUTF(row.getKey()); out.writeLong(row.getValue().getExpiryTimestampMillis());
                byte[] value = row.getValue().getValue().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                out.writeInt(value.length); out.write(value);
            }
            out.writeInt(transactions.size());
            for (TransactionRecord record : transactions.values()) writeTransactionRecord(out, record);
            writeStringList(out, migrationIntent);
        }
        byte[] body = payload.toByteArray();
        CRC32 crc = new CRC32(); crc.update(body);
        ByteArrayOutputStream result = new ByteArrayOutputStream(body.length + 4);
        result.write(body);
        try (DataOutputStream out = new DataOutputStream(result)) { out.writeInt((int) crc.getValue()); }
        if (result.size() > MAX_SNAPSHOT_BYTES) throw new IOException("Raft snapshot exceeds configured maximum size");
        return result.toByteArray();
    }

    private SnapshotHeader restoreSnapshot(byte[] bytes) throws IOException {
        SnapshotData data = decodeSnapshot(bytes);
        store.replaceSnapshot(data.entries());
        maintenanceFrozen = data.frozen(); ownershipEpoch = data.ownershipEpoch();
        ownershipGroups = data.ownershipGroups();
        migrationIntent = data.migrationIntent();
        voters.clear(); voters.addAll(data.voters().keySet());
        if (data.voters().containsKey(nodeId)) selfEndpoint = data.voters().get(nodeId);
        peers.clear(); data.voters().forEach((id, address) -> { if (!id.equals(nodeId)) peers.put(id, address); });
        if (data.jointVoters() == null) jointVoters = null;
        else jointVoters = new java.util.LinkedHashSet<>(data.jointVoters().keySet());
        pendingVoterPeers = data.pendingVoters();
        if (jointVoters != null && pendingVoterPeers == null) pendingVoterPeers = data.jointVoters();
        transactions.clear(); preparedKeyOwners.clear();
        for (TransactionRecord record : data.transactions()) {
            transactions.put(record.id(), record);
            if (record.prepared()) for (String key : record.changes().keySet()) preparedKeyOwners.put(key, record.id());
        }
        return data.header();
    }

    private static SnapshotData decodeSnapshot(byte[] bytes) throws IOException {
        if (bytes.length < 24 || bytes.length > MAX_SNAPSHOT_BYTES) throw new IOException("Invalid Raft snapshot length");
        int bodyLength = bytes.length - 4;
        CRC32 crc = new CRC32(); crc.update(bytes, 0, bodyLength);
        int supplied = java.nio.ByteBuffer.wrap(bytes, bodyLength, 4).getInt();
        if ((int) crc.getValue() != supplied) throw new IOException("Raft snapshot checksum mismatch");
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes, 0, bodyLength))) {
            if (in.readInt() != 0x4b565331) throw new IOException("Unknown Raft snapshot format");
            long index = in.readLong(), term = in.readLong();
            if (index < 0 || term < 0 || (index == 0 && term != 0)) throw new IOException("Invalid Raft snapshot boundary");
            boolean frozen = in.readBoolean(); long epoch = in.readLong();
            if (epoch < 0) throw new IOException("Invalid ownership epoch in snapshot");
            List<String> groups = readStringList(in, 100_000);
            Map<String, InetSocketAddress> voters = readEndpointMap(in);
            Map<String, InetSocketAddress> joint = in.readBoolean() ? readEndpointMap(in) : null;
            Map<String, InetSocketAddress> pending = in.readBoolean() ? readEndpointMap(in) : null;
            int count = in.readInt(); if (count < 0 || count > 100_000) throw new IOException("Invalid snapshot row count");
            Map<String, ValueEntry> entries = new LinkedHashMap<>();
            for (int i = 0; i < count; i++) {
                String key = in.readUTF(); long expiry = in.readLong(); int length = in.readInt();
                if (length < 0 || length > 1024 * 1024 || length > in.available()) throw new IOException("Invalid snapshot value length");
                byte[] value = in.readNBytes(length);
                if (entries.putIfAbsent(key, new ValueEntry(new String(value, java.nio.charset.StandardCharsets.UTF_8), expiry)) != null)
                    throw new IOException("Duplicate snapshot key");
            }
            int txnCount = in.readInt(); if (txnCount < 0 || txnCount > 1_000_000) throw new IOException("Invalid snapshot transaction count");
            List<TransactionRecord> records = new java.util.ArrayList<>(txnCount);
            for (int i = 0; i < txnCount; i++) records.add(readTransactionRecord(in));
            List<String> migrationIntent = in.available() == 0 ? List.of() : readStringList(in, 100_000);
            if (in.available() != 0) throw new IOException("Trailing Raft snapshot data");
            return new SnapshotData(new SnapshotHeader(index, term), entries, frozen, epoch, groups,
                    voters, joint, pending, List.copyOf(records), migrationIntent);
        } catch (java.io.EOFException | IllegalArgumentException e) { throw new IOException("Malformed Raft snapshot", e); }
    }

    private void persistSnapshot(byte[] bytes) throws IOException {
        Path temporary = snapshotFile.resolveSibling(snapshotFile.getFileName() + ".tmp");
        try (FileChannel out = FileChannel.open(temporary, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            ByteBuffer buffer = ByteBuffer.wrap(bytes); while (buffer.hasRemaining()) out.write(buffer); out.force(true);
        }
        try { Files.move(temporary, snapshotFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING); }
        catch (java.nio.file.AtomicMoveNotSupportedException e) { Files.move(temporary, snapshotFile, StandardCopyOption.REPLACE_EXISTING); }
    }

    private static int positiveSetting(String name, int fallback) {
        String raw = System.getenv(name);
        if (raw == null || raw.isBlank()) return fallback;
        try { int value = Integer.parseInt(raw); if (value < 1) throw new IllegalArgumentException(name + " must be positive"); return value; }
        catch (NumberFormatException e) { throw new IllegalArgumentException(name + " must be an integer", e); }
    }

    private void sendSnapshot(String peerId, InetSocketAddress address) throws IOException {
        byte[] snapshot = Files.readAllBytes(snapshotFile);
        SnapshotHeader header = decodeSnapshot(snapshot).header();
        int chunks = (snapshot.length + SNAPSHOT_CHUNK_BYTES - 1) / SNAPSHOT_CHUNK_BYTES;
        String transferId = UUID.randomUUID().toString();
        for (int chunk = 0; chunk < chunks; chunk++) {
            int start = chunk * SNAPSHOT_CHUNK_BYTES, length = Math.min(SNAPSHOT_CHUNK_BYTES, snapshot.length - start);
            final int chunkNumber = chunk;
            byte[] payload = encode(out -> {
                out.writeByte(25); out.writeUTF(transferId); out.writeLong(header.index()); out.writeLong(header.term());
                out.writeLong(term); out.writeInt(snapshot.length); out.writeInt(chunkNumber); out.writeInt(length); out.write(snapshot, start, length);
            });
            RpcMessage response = call(peerId, address, RpcMessage.Type.REQUEST, payload);
            DataInputStream result = input(response); boolean ok = result.readBoolean(); result.readLong(); result.readBoolean(); result.readInt();
            if (!ok) throw new IOException("Follower rejected snapshot: " + result.readUTF());
        }
    }

    private synchronized void installSnapshotChunk(String source, byte[] command) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(command)); in.readUnsignedByte();
        String id = in.readUTF(); long index = in.readLong(), includedTerm = in.readLong(), leaderTerm = in.readLong();
        int total = in.readInt(), chunk = in.readInt(), length = in.readInt();
        if (total <= 0 || total > MAX_SNAPSHOT_BYTES || chunk < 0 || length < 0
                || length > SNAPSHOT_CHUNK_BYTES || length > in.available()) throw new IOException("Invalid snapshot chunk");
        byte[] part = in.readNBytes(length); if (in.available() != 0) throw new IOException("Trailing snapshot chunk bytes");
        if (leaderTerm < term) throw new IOException("Stale snapshot from an older Raft term");
        if (chunk == 0) snapshotTransfer = new SnapshotTransfer(id, source, index, includedTerm, total, 0, new ByteArrayOutputStream(total));
        SnapshotTransfer transfer = snapshotTransfer;
        if (transfer == null || !transfer.id().equals(id) || !transfer.source().equals(source)
                || transfer.index() != index || transfer.term() != includedTerm || transfer.totalBytes() != total
                || transfer.nextChunk() != chunk) throw new IOException("Out-of-order or inconsistent snapshot transfer");
        transfer.bytes().write(part);
        snapshotTransfer = new SnapshotTransfer(id, source, index, includedTerm, total, chunk + 1, transfer.bytes());
        if (transfer.bytes().size() > total) { snapshotTransfer = null; throw new IOException("Snapshot transfer exceeded declared length"); }
        if (transfer.bytes().size() == total) {
            byte[] complete = transfer.bytes().toByteArray(); SnapshotData decoded = decodeSnapshot(complete);
            if (decoded.header().index() != index || decoded.header().term() != includedTerm) throw new IOException("Snapshot boundary mismatch");
            if (index < commitIndex) {
                // A peer can already have committed a suffix beyond this checkpoint.
                // Keep that state and log; ordinary AppendEntries will reconcile the suffix.
                if (index >= log.baseIndex() && log.get(index).map(entry -> entry.getTerm() != includedTerm).orElse(true))
                    throw new IOException("Stale snapshot conflicts with a locally committed log");
            } else {
                persistSnapshot(complete); restoreSnapshot(complete); log.installSnapshot(index, includedTerm);
                if (leaderTerm > term) votedFor = null;
                term = Math.max(term, leaderTerm); leaderId = source; role = Role.FOLLOWER;
                commitIndex = index; lastApplied = index;
                metadata.save(term, votedFor, commitIndex); refreshPendingConfiguration();
            }
            snapshotTransfer = null;
        }
    }

    private Map<String, InetSocketAddress> voterEndpoints(Set<String> ids, Map<String, InetSocketAddress> known) throws IOException {
        Map<String, InetSocketAddress> result = new LinkedHashMap<>();
        for (String id : ids) {
            InetSocketAddress endpoint = id.equals(nodeId) ? transportAddress() : known.get(id);
            if (endpoint == null) throw new IOException("Missing endpoint for voter " + id);
            result.put(id, endpoint);
        }
        return result;
    }

    private InetSocketAddress transportAddress() {
        int port = selfEndpoint.getPort() == 0 ? getPort() : selfEndpoint.getPort();
        return new InetSocketAddress(selfEndpoint.getHostString(), port);
    }

    private static void writeEndpointMap(DataOutputStream out, Map<String, InetSocketAddress> endpoints) throws IOException {
        out.writeInt(endpoints.size());
        for (Map.Entry<String, InetSocketAddress> endpoint : endpoints.entrySet()) {
            out.writeUTF(endpoint.getKey()); out.writeUTF(endpoint.getValue().getHostString()); out.writeInt(endpoint.getValue().getPort());
        }
    }

    private static Map<String, InetSocketAddress> readEndpointMap(DataInputStream in) throws IOException {
        int count = in.readInt(); if (count <= 0 || count > 10_000) throw new IOException("Invalid snapshot voter count");
        Map<String, InetSocketAddress> endpoints = new LinkedHashMap<>();
        for (int i = 0; i < count; i++) {
            String id = in.readUTF(), host = in.readUTF(); int port = in.readInt();
            if (port <= 0 || port > 65535 || endpoints.putIfAbsent(id, new InetSocketAddress(host, port)) != null)
                throw new IOException("Invalid snapshot voter endpoint");
        }
        return endpoints;
    }

    private static void writeStringList(DataOutputStream out, List<String> strings) throws IOException {
        out.writeInt(strings.size()); for (String string : strings) out.writeUTF(string);
    }

    private static List<String> readStringList(DataInputStream in, int max) throws IOException {
        int count = in.readInt(); if (count < 0 || count > max) throw new IOException("Invalid snapshot list count");
        List<String> strings = new java.util.ArrayList<>(count); for (int i = 0; i < count; i++) strings.add(in.readUTF());
        return List.copyOf(strings);
    }

    private RpcMessage handleRpc(RpcMessage request) throws Exception {
        if (!request.getTargetNodeId().equals(nodeId)) throw new IOException("Wrong target node");
        if (!request.getSourceNodeId().equals(nodeId) && !peers.containsKey(request.getSourceNodeId()))
            throw new IOException("Unknown source node");
        DataInputStream envelope = input(request);
        String incomingGroup = envelope.readUTF();
        if (!groupId.equals(incomingGroup)) throw new IOException("Raft group mismatch");
        boolean authenticated = envelope.readBoolean();
        String configuredToken = System.getenv("KV_CLUSTER_TOKEN");
        if (configuredToken != null && !configuredToken.isBlank()) {
            if (!authenticated) throw new IOException("Cluster authentication required");
            byte[] expected = configuredToken.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            byte[] supplied = envelope.readNBytes(envelope.readInt());
            if (supplied.length != expected.length || !java.security.MessageDigest.isEqual(expected, supplied))
                throw new IOException("Invalid cluster authentication token");
        } else if (authenticated) throw new IOException("Cluster token is not configured on this node");
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
        boolean eligible = isEligibleVoter(request.getSourceNodeId());
        boolean upToDate = candidateLastTerm > log.lastTerm() ||
                candidateLastTerm == log.lastTerm() && candidateIndex >= log.lastIndex();
        boolean grant = eligible && candidateTerm == term && upToDate
                && (votedFor == null || votedFor.equals(request.getSourceNodeId()));
        if (grant) { votedFor = request.getSourceNodeId(); metadata.save(term, votedFor, commitIndex); leaderId = null; role = Role.FOLLOWER; resetElectionDeadline(); }
        long currentTerm = term; boolean result = grant;
        return response(request, RpcMessage.Type.VOTE_RESPONSE, encode(out -> { out.writeLong(currentTerm); out.writeBoolean(result); }));
    }

    private synchronized RpcMessage handleAppend(RpcMessage request) throws IOException {
        DataInputStream in = input(request); long leaderTerm = in.readLong(), previousIndex = in.readLong(), previousTerm = in.readLong(), leaderCommit = in.readLong();
        if (leaderTerm > term) stepDown(leaderTerm);
        boolean success = false;
        if (leaderTerm == term && previousIndex <= log.lastIndex() && previousIndex >= log.baseIndex() &&
                (previousIndex == 0 || previousIndex == log.baseIndex() && log.baseTerm() == previousTerm
                        || log.get(previousIndex).map(e -> e.getTerm() == previousTerm).orElse(false))) {
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
                refreshPendingConfiguration();
            }
            long updatedCommit = Math.min(leaderCommit, log.lastIndex());
            if (updatedCommit > commitIndex) {
                commitIndex = updatedCommit; metadata.save(term, votedFor, commitIndex); applyCommitted();
                refreshPendingConfiguration();
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

    private static Map<String, InetSocketAddress> readVoterConfig(byte[] command) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(command));
        if (in.readUnsignedByte() != 18) throw new IOException("Not a voter configuration entry");
        Map<String, InetSocketAddress> result = readVoterConfig(in);
        if (in.available() != 0) throw new IOException("Trailing bytes in voter configuration");
        return result;
    }

    private static Map<String, InetSocketAddress> readVoterConfig(DataInputStream in) throws IOException {
        int count = in.readInt();
        if (count <= 0 || count > 10_000) throw new IOException("Invalid voter count");
        Map<String, InetSocketAddress> result = new LinkedHashMap<>();
        for (int i = 0; i < count; i++) {
            String id = in.readUTF(), host = in.readUTF(); int port = in.readInt();
            if (port < 1 || port > 65535 || result.putIfAbsent(id, new InetSocketAddress(host, port)) != null)
                throw new IOException("Invalid or duplicate voter endpoint");
        }
        return result;
    }

    /** Applies the latest uncommitted configuration to this server's election and quorum rules. */
    private synchronized void refreshPendingConfiguration() throws IOException {
        uncommittedJointVoters = null;
        uncommittedFinalVoters = null;
        Map<String, InetSocketAddress> latestTarget = jointVoters == null ? null : pendingVoterPeers;
        for (long index = commitIndex + 1; index <= log.lastIndex(); index++) {
            RaftLogEntry entry = log.get(index).orElseThrow(() -> new IOException("Missing Raft entry during config refresh"));
            byte[] command = entry.getCommand();
            if (command.length == 0) continue;
            int op = command[0] & 0xff;
            if (op == 18) {
                latestTarget = readVoterConfig(command);
                uncommittedJointVoters = new java.util.LinkedHashSet<>(latestTarget.keySet());
                uncommittedFinalVoters = null;
            } else if (op == 19 && latestTarget != null) {
                uncommittedFinalVoters = new java.util.LinkedHashSet<>(latestTarget.keySet());
                uncommittedJointVoters = null;
            }
        }
        if (latestTarget != null) {
            pendingVoterPeers = latestTarget;
            for (Map.Entry<String, InetSocketAddress> voter : latestTarget.entrySet())
                if (!voter.getKey().equals(nodeId)) peers.put(voter.getKey(), voter.getValue());
        }
    }

    private void applyCommitted() throws IOException {
        while (lastApplied < commitIndex) {
            RaftLogEntry entry = log.get(++lastApplied).orElseThrow(() -> new IOException("Missing committed entry"));
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(entry.getCommand()));
            int op = in.readUnsignedByte();
            if (op == 0) continue;
            if (op == 7) { maintenanceFrozen = in.readUnsignedByte() != 0; continue; }
            if (op == 21) { migrationIntent = readStringList(in, 100_000); continue; }
            if (op == 18) {
                Map<String, InetSocketAddress> target = readVoterConfig(in);
                jointVoters = new java.util.LinkedHashSet<>(target.keySet());
                pendingVoterPeers = target;
                for (Map.Entry<String, InetSocketAddress> voter : target.entrySet())
                    if (!voter.getKey().equals(nodeId)) peers.put(voter.getKey(), voter.getValue());
                continue;
            }
            if (op == 19) {
                if (jointVoters == null || pendingVoterPeers == null)
                    throw new IOException("Final voter configuration has no committed joint phase");
                voters.clear(); voters.addAll(jointVoters);
                peers.clear();
                for (Map.Entry<String, InetSocketAddress> voter : pendingVoterPeers.entrySet())
                    if (!voter.getKey().equals(nodeId)) peers.put(voter.getKey(), voter.getValue());
                jointVoters = null; pendingVoterPeers = null;
                if (!voters.contains(nodeId)) { role = Role.FOLLOWER; leaderId = null; }
                continue;
            }
            if (op == 20) {
                long nextEpoch = in.readLong();
                if (nextEpoch != ownershipEpoch + 1) throw new IOException("Non-monotonic ownership epoch in committed log");
                ownershipGroups = readStrings(in);
                ownershipEpoch = nextEpoch;
                continue;
            }
            if (op == 11) {
                String id = in.readUTF(), digest = in.readUTF(); List<String> participants = readStrings(in);
                TransactionRecord existing = transactions.get(id);
                if (existing != null && (!existing.digest().equals(digest)
                        || !existing.participants().equals(participants)))
                    throw new IOException("Transaction ID reused with different coordinator metadata: " + id);
                if (existing == null) transactions.put(id,
                        new TransactionRecord(id, digest, participants, Map.of(), null, false, false, entry.getTerm()));
                continue;
            }
            if (op == 12) {
                String id = in.readUTF(), digest = in.readUTF(); List<String> participants = readStrings(in);
                int count = in.readInt(); Map<String, String> changes = new LinkedHashMap<>();
                for (int i = 0; i < count; i++) changes.put(in.readUTF(), in.readBoolean() ? in.readUTF() : null);
                validatePrepareAvailable(id, changes.keySet());
                TransactionRecord existing = transactions.get(id);
                if (existing != null && (!existing.digest().equals(digest)
                        || !existing.participants().equals(participants)))
                    throw new IOException("Transaction prepare metadata mismatch: " + id);
                for (String key : changes.keySet()) preparedKeyOwners.put(key, id);
                transactions.put(id, new TransactionRecord(id, digest, participants, changes,
                        existing == null ? null : existing.decision(), true, false,
                        existing == null ? entry.getTerm() : existing.coordinatorTerm()));
                continue;
            }
            if (op == 13) {
                String id = in.readUTF(), digest = in.readUTF(); List<String> participants = readStrings(in);
                boolean commit = in.readBoolean();
                TransactionRecord existing = transactions.get(id);
                if (existing != null && (!existing.digest().equals(digest)
                        || !existing.participants().equals(participants)))
                    throw new IOException("Transaction decision metadata mismatch: " + id);
                if (existing != null && existing.decision() != null && existing.decision() != commit)
                    throw new IOException("Transaction decision cannot be changed: " + id);
                transactions.put(id, new TransactionRecord(id, digest, participants,
                        existing == null ? Map.of() : existing.changes(), commit,
                        existing != null && existing.prepared(), existing != null && existing.applied(),
                        existing == null ? entry.getTerm() : existing.coordinatorTerm()));
                continue;
            }
            if (op == 14 || op == 15) {
                String id = in.readUTF(), digest = in.readUTF();
                TransactionRecord existing = transactions.get(id);
                if (existing == null || !existing.digest().equals(digest))
                    throw new IOException("Unknown or mismatched prepared transaction: " + id);
                if (op == 14) {
                    if (!existing.prepared() && existing.applied()) continue;
                    if (!existing.prepared()) throw new IOException("Commit has no prepared participant intent: " + id);
                    store.applyAtomicChanges(existing.changes());
                    for (String key : existing.changes().keySet()) preparedKeyOwners.remove(key, id);
                    transactions.put(id, new TransactionRecord(id, digest, existing.participants(),
                            existing.changes(), true, false, true, existing.coordinatorTerm()));
                } else {
                    if (existing.applied()) throw new IOException("Cannot abort an applied transaction: " + id);
                    for (String key : existing.changes().keySet()) preparedKeyOwners.remove(key, id);
                    transactions.put(id, new TransactionRecord(id, digest, existing.participants(),
                            existing.changes(), false, false, false, existing.coordinatorTerm()));
                }
                continue;
            }
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
            if (op == 10) {
                int count = in.readInt();
                if (count < 0 || count > 1_000_000) throw new IOException("Invalid delete batch count");
                String[] keys = new String[count];
                for (int i = 0; i < count; i++) keys[i] = in.readUTF();
                store.deleteBatchAtomic(keys);
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

    private synchronized void stepDown(long newTerm) throws IOException {
        if (newTerm > term) { term = newTerm; votedFor = null; metadata.save(term, null, commitIndex); }
        role = Role.FOLLOWER; leaderId = null;
        resetElectionDeadline();
    }

    private synchronized void verifyRequestOwnershipEpoch() throws IOException {
        Long expected = requestOwnershipEpoch.get();
        if (ownershipEpoch > 0 && expected == null)
            throw new IOException("Ownership epoch token is required for this shard");
        if (expected != null && expected != ownershipEpoch)
            throw new IOException("Stale shard owner: request epoch=" + expected + ", current epoch=" + ownershipEpoch);
    }

    private static boolean requiresOwnerFence(byte[] command) {
        if (command.length == 0) return false;
        return switch (command[0] & 0xff) {
            case 1, 2, 3, 4, 8, 9, 10, 12 -> true;
            default -> false;
        };
    }
    private int majority(Set<String> configuration) { return configuration.size() / 2 + 1; }
    private boolean isEligibleVoter(String candidateId) {
        if (uncommittedFinalVoters != null) return uncommittedFinalVoters.contains(candidateId);
        if (uncommittedJointVoters != null)
            return voters.contains(candidateId) || uncommittedJointVoters.contains(candidateId);
        return voters.contains(candidateId) || jointVoters != null && jointVoters.contains(candidateId);
    }
    private boolean hasQuorum(Set<String> acknowledgements) {
        if (uncommittedFinalVoters != null)
            return acknowledgements.stream().filter(uncommittedFinalVoters::contains).count() >= majority(uncommittedFinalVoters);
        Set<String> newSet = uncommittedJointVoters != null ? uncommittedJointVoters : jointVoters;
        return acknowledgements.stream().filter(voters::contains).count() >= majority(voters)
                && (newSet == null || acknowledgements.stream().filter(newSet::contains).count() >= majority(newSet));
    }
    private boolean hasQuorumForCommand(Set<String> acknowledgements, byte[] command) throws IOException {
        if (command.length > 0 && command[0] == 18) {
            Set<String> target = readVoterConfig(command).keySet();
            return acknowledgements.stream().filter(voters::contains).count() >= majority(voters)
                    && acknowledgements.stream().filter(target::contains).count() >= majority(target);
        }
        if (command.length > 0 && command[0] == 19 && jointVoters != null)
            return acknowledgements.stream().filter(jointVoters::contains).count() >= majority(jointVoters);
        return hasQuorum(acknowledgements);
    }

    private synchronized void validatePrepareAvailable(String id, Set<String> keys) throws IOException {
        TransactionRecord existing = transactions.get(id);
        for (String key : keys) {
            String owner = preparedKeyOwners.get(key);
            if (owner != null && !owner.equals(id))
                throw new IOException("Key is locked by prepared transaction " + owner);
        }
        if (existing != null && existing.prepared()) {
            if (!existing.changes().keySet().equals(keys))
                throw new IOException("Transaction ID reused with different prepared keys: " + id);
        }
    }

    private synchronized void ensureKeyNotPrepared(String key) throws IOException {
        String transaction = preparedKeyOwners.get(key);
        if (transaction != null) throw new IOException("Key is locked by prepared transaction " + transaction);
    }

    private synchronized void validateTransactionCommand(byte[] command) throws IOException {
        if (command.length == 0) return;
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(command));
        int op = in.readUnsignedByte();
        if (op == 11) {
            String id = in.readUTF(), digest = in.readUTF(); List<String> participants = readStrings(in);
            TransactionRecord old = transactions.get(id);
            if (old != null && (!old.digest().equals(digest) || !old.participants().equals(participants)))
                throw new IOException("Transaction ID already exists with different metadata: " + id);
        } else if (op == 12) {
            String id = in.readUTF(), digest = in.readUTF(); List<String> participants = readStrings(in);
            int count = in.readInt(); Map<String, String> changes = new LinkedHashMap<>();
            for (int i = 0; i < count; i++) changes.put(in.readUTF(), in.readBoolean() ? in.readUTF() : null);
            TransactionRecord old = transactions.get(id);
            if (old != null && (!old.digest().equals(digest) || !old.participants().equals(participants)))
                throw new IOException("Transaction prepare metadata mismatch: " + id);
            validatePrepareAvailable(id, changes.keySet());
        } else if (op == 13) {
            String id = in.readUTF(), digest = in.readUTF(); List<String> participants = readStrings(in);
            boolean decision = in.readBoolean(); TransactionRecord old = transactions.get(id);
            if (old == null || !old.digest().equals(digest) || !old.participants().equals(participants))
                throw new IOException("Transaction coordinator record is missing: " + id);
            if (old.decision() != null && old.decision() != decision)
                throw new IOException("Transaction decision cannot be changed: " + id);
        } else if (op == 14 || op == 15) {
            String id = in.readUTF(), digest = in.readUTF(); TransactionRecord old = transactions.get(id);
            if (old == null || !old.digest().equals(digest))
                throw new IOException("Transaction participant intent is missing: " + id);
            if (op == 14 && !old.prepared() && !old.applied())
                throw new IOException("Commit has no prepared participant intent: " + id);
            if (op == 15 && !old.prepared() && !Boolean.FALSE.equals(old.decision()))
                throw new IOException("Abort has no prepared participant intent: " + id);
            if (op == 15 && old.applied()) throw new IOException("Cannot abort an applied transaction: " + id);
        }
    }

    private synchronized void validateNoPreparedConflict(byte[] command) throws IOException {
        if (command.length == 0) return;
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(command));
        int op = in.readUnsignedByte();
        List<String> keys = new java.util.ArrayList<>();
        if (op == 1 || op == 2 || op == 3 || op == 8 || op == 9) keys.add(in.readUTF());
        else if (op == 4) {
            int count = in.readInt();
            for (int i = 0; i < count; i++) {
                keys.add(in.readUTF()); int length = in.readInt();
                if (length < 0 || length > MAX_COMMAND_BYTES) throw new IOException("Invalid batch value length");
                in.skipNBytes(length);
            }
        } else if (op == 10) {
            int count = in.readInt();
            for (int i = 0; i < count; i++) keys.add(in.readUTF());
        }
        for (String key : keys) {
            String transaction = preparedKeyOwners.get(key);
            if (transaction != null) throw new IOException("Key is locked by prepared transaction " + transaction);
        }
    }

    private static void writeStrings(DataOutputStream out, List<String> values) throws IOException {
        out.writeInt(values.size());
        for (String value : values) out.writeUTF(value);
    }

    private static List<String> readStrings(DataInputStream in) throws IOException {
        int count = in.readInt();
        if (count <= 0 || count > 100_000) throw new IOException("Invalid transaction participant count");
        List<String> values = new java.util.ArrayList<>(count);
        for (int i = 0; i < count; i++) values.add(in.readUTF());
        if (new java.util.LinkedHashSet<>(values).size() != values.size())
            throw new IOException("Duplicate transaction participant");
        return List.copyOf(values);
    }

    private static void writeTransactionRecord(DataOutputStream out, TransactionRecord record) throws IOException {
        out.writeUTF(record.id()); out.writeUTF(record.digest()); writeStrings(out, record.participants());
        out.writeInt(record.changes().size());
        for (Map.Entry<String, String> change : record.changes().entrySet()) {
            out.writeUTF(change.getKey()); out.writeBoolean(change.getValue() != null);
            if (change.getValue() != null) out.writeUTF(change.getValue());
        }
        out.writeByte(record.decision() == null ? -1 : record.decision() ? 1 : 0);
        out.writeBoolean(record.prepared()); out.writeBoolean(record.applied()); out.writeLong(record.coordinatorTerm());
    }

    private static TransactionRecord readTransactionRecord(DataInputStream in) throws IOException {
        String id = in.readUTF(), digest = in.readUTF(); List<String> participants = readStrings(in);
        int count = in.readInt();
        if (count < 0 || count > 1_000_000) throw new IOException("Invalid transaction change count");
        Map<String, String> changes = new LinkedHashMap<>();
        for (int i = 0; i < count; i++) changes.put(in.readUTF(), in.readBoolean() ? in.readUTF() : null);
        int decisionByte = in.readByte();
        if (decisionByte < -1 || decisionByte > 1) throw new IOException("Invalid transaction decision");
        Boolean decision = decisionByte < 0 ? null : decisionByte == 1;
        return new TransactionRecord(id, digest, participants, changes, decision,
                in.readBoolean(), in.readBoolean(), in.readLong());
    }

    private RpcMessage call(String target, InetSocketAddress address, RpcMessage.Type type, byte[] payload) throws IOException {
        String token = System.getenv("KV_CLUSTER_TOKEN");
        byte[] scoped = encode(out -> {
            out.writeUTF(groupId); out.writeBoolean(token != null && !token.isBlank());
            if (token != null && !token.isBlank()) {
                byte[] bytes = token.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                out.writeInt(bytes.length); out.write(bytes);
            }
            out.write(payload);
        });
        return new RpcTransportClient(address, rpcConnectTimeoutMillis, rpcReadTimeoutMillis)
                .call(new RpcMessage(type, UUID.randomUUID().toString(), nodeId, target, scoped));
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
        catch (Exception failure) {
            if (Boolean.getBoolean("kv.raft.debug"))
                System.err.println("Raft " + nodeId + " tick failed: " + failure);
        }
    }

    private void tick() throws IOException {
        final boolean electionDue;
        final List<Map.Entry<String, InetSocketAddress>> heartbeatPeers;
        final RaftLogEntry heartbeatEntry;
        synchronized (this) {
            if (!running) return;
            long now = System.nanoTime();
            electionDue = role != Role.LEADER && now - lastContactNanos >= electionTimeoutNanos;
            if (role == Role.LEADER && now - lastHeartbeatNanos >= TimeUnit.MILLISECONDS.toNanos(100)) {
                lastHeartbeatNanos = now;
                heartbeatPeers = new java.util.ArrayList<>(peers.entrySet());
                heartbeatEntry = log.get(log.lastIndex()).orElse(null);
            } else {
                heartbeatPeers = List.of(); heartbeatEntry = null;
            }
        }
        if (electionDue) startElection();
        for (Map.Entry<String, InetSocketAddress> peer : heartbeatPeers) {
            try { replicate(peer.getKey(), peer.getValue(), heartbeatEntry); }
            catch (IOException failure) {
                if (Boolean.getBoolean("kv.raft.debug"))
                    System.err.println("Raft " + nodeId + " heartbeat to " + peer.getKey() + " failed: " + failure);
            }
        }
    }

    private void resetElectionDeadline() {
        lastContactNanos = System.nanoTime();
        electionTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(
                ThreadLocalRandom.current().nextLong(electionTimeoutMinMillis, electionTimeoutMaxMillis));
    }

    @Override public synchronized void close() throws IOException {
        running = false; role = Role.FOLLOWER; leaderId = null;
        scheduler.shutdownNow(); transport.close(); log.close();
    }
}
