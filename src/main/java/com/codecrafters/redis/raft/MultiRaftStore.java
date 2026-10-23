package com.codecrafters.redis.raft;

import com.codecrafters.redis.sharding.ConsistentHash;
import com.codecrafters.redis.storage.KeyValueAccess;
import com.codecrafters.redis.storage.ValueEntry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.UUID;

/** Routes each key to one independently replicated Raft group using a virtual-node hash ring. */
public final class MultiRaftStore implements KeyValueAccess, AutoCloseable {
    public record LeadershipStatus(String leaderId, boolean localLeader, long term,
                                   long commitIndex, long ownershipEpoch) { }
    private final Map<String, RaftGroup> groups;
    private final Map<String, Long> ownershipEpochs = new LinkedHashMap<>();
    private volatile ConsistentHash<String> ring;
    private final boolean followerReads;
    private final long maxFollowerCommitLag;
    private final int virtualNodesPerGroup;
    private final ReentrantReadWriteLock routingLock = new ReentrantReadWriteLock(true);

    public MultiRaftStore(Map<String, RaftGroup> groups, int virtualNodesPerGroup, boolean followerReads) {
        this(groups, virtualNodesPerGroup, followerReads, 0);
    }

    public MultiRaftStore(Map<String, RaftGroup> groups, int virtualNodesPerGroup,
                          boolean followerReads, long maxFollowerCommitLag) {
        if (groups == null || groups.isEmpty()) throw new IllegalArgumentException("At least one Raft group is required");
        this.groups = Collections.unmodifiableMap(new LinkedHashMap<>(groups));
        this.groups.forEach((id, group) -> ownershipEpochs.put(id, group.getOwnershipEpoch()));
        if (virtualNodesPerGroup <= 0) throw new IllegalArgumentException("virtual node count must be positive");
        this.virtualNodesPerGroup = virtualNodesPerGroup;
        this.ring = new ConsistentHash<>(virtualNodesPerGroup);
        List<String> persistedOwnership = null;
        long persistedEpoch = -1;
        boolean allFrozen = this.groups.values().stream().allMatch(RaftGroup::isMaintenanceFrozen);
        for (RaftGroup group : this.groups.values()) {
            List<String> manifest = group.getOwnershipGroups();
            if (manifest.isEmpty()) continue;
            if (persistedOwnership != null && !persistedOwnership.equals(manifest) && !allFrozen)
                throw new IllegalStateException("Raft groups disagree on durable ownership manifest; frozen recovery is required");
            if (group.getOwnershipEpoch() > persistedEpoch) {
                persistedOwnership = manifest;
                persistedEpoch = group.getOwnershipEpoch();
            }
        }
        List<String> initialOwners = persistedOwnership == null ? List.copyOf(this.groups.keySet()) : persistedOwnership;
        if (initialOwners.isEmpty() || !this.groups.keySet().containsAll(initialOwners))
            throw new IllegalStateException("Durable ownership manifest references an unprovisioned group");
        initialOwners.forEach(ring::addNode);
        this.followerReads = followerReads;
        if (maxFollowerCommitLag < 0) throw new IllegalArgumentException("follower lag bound cannot be negative");
        this.maxFollowerCommitLag = maxFollowerCommitLag;
    }

    public String groupFor(String key) { return ring.getNode(Objects.requireNonNull(key, "key")); }

    /** Current per-group leadership and durable progress for authenticated diagnostics. */
    public Map<String, LeadershipStatus> leadershipStatus() {
        Map<String, LeadershipStatus> status = new LinkedHashMap<>();
        groups.forEach((id, group) -> status.put(id, new LeadershipStatus(group.getLeaderId(),
                group.isLeader(), group.getTerm(), group.getCommitIndex(), group.getOwnershipEpoch())));
        return Collections.unmodifiableMap(status);
    }

    /**
     * Rebalances between Raft groups already provisioned on every cluster node.
     * All groups are quorum-frozen before copying, the new ring is published only
     * after destination writes commit, and source copies are removed afterward.
     * Each cluster process must be administered with the same group set and change.
     */
    public void rebalanceGroups(java.util.Collection<String> activeGroupIds) throws IOException {
        Objects.requireNonNull(activeGroupIds, "activeGroupIds");
        routingLock.writeLock().lock();
        java.util.List<RaftGroup> frozen = new java.util.ArrayList<>();
        java.util.List<Move> copied = new java.util.ArrayList<>();
        boolean routingPublished = false;
        boolean ownershipTransitionStarted = false;
        try {
            java.util.LinkedHashSet<String> desired = new java.util.LinkedHashSet<>(activeGroupIds);
            if (desired.isEmpty() || desired.size() != activeGroupIds.size() || !groups.keySet().containsAll(desired))
                throw new IllegalArgumentException("Active group set must contain unique provisioned groups");
            ConsistentHash<String> next = new ConsistentHash<>(virtualNodesPerGroup);
            desired.forEach(next::addNode);
            if (new java.util.LinkedHashSet<>(ringNodes()).equals(desired)
                    && groups.values().stream().noneMatch(RaftGroup::isMaintenanceFrozen)
                    && groups.values().stream().allMatch(group -> group.getOwnershipGroups().isEmpty()
                    || new java.util.LinkedHashSet<>(group.getOwnershipGroups()).equals(desired))) return;
            java.util.Set<String> currentOwners = new java.util.LinkedHashSet<>(ringNodes());
            for (RaftGroup group : groups.values()) {
                List<String> persisted = group.getOwnershipGroups();
                if (group.isMaintenanceFrozen() && !persisted.isEmpty()
                        && !new java.util.LinkedHashSet<>(persisted).equals(currentOwners)
                        && !new java.util.LinkedHashSet<>(persisted).equals(desired))
                    throw new IOException("A different interrupted ownership transition must be resolved first");
            }
            try {
                for (RaftGroup group : groups.values()) {
                    group.setMaintenanceFrozen(true);
                    frozen.add(group);
                }
            } catch (IOException freezeFailure) {
                unfreezeAll(frozen, freezeFailure);
                throw freezeFailure;
            }

            java.util.List<String> oldOwners = ringNodes();
            Map<String, Map<String, ValueEntry>> snapshots = new LinkedHashMap<>();
            for (String owner : oldOwners) snapshots.put(owner, groups.get(owner).snapshotEntriesWithReadIndex());
            for (Map.Entry<String, Map<String, ValueEntry>> shard : snapshots.entrySet()) {
                for (Map.Entry<String, ValueEntry> entry : shard.getValue().entrySet()) {
                    String destination = next.getNode(entry.getKey());
                    if (!shard.getKey().equals(destination)) {
                        fenced(destination, () -> groups.get(destination).putEntry(entry.getKey(), entry.getValue()));
                        copied.add(new Move(shard.getKey(), destination, entry.getKey()));
                    }
                }
            }

            // Advance every shard's durable fence while all groups remain frozen.
            // Routers that cached the old per-group epoch can no longer read or write.
            ownershipTransitionStarted = true;
            long targetEpoch = groups.values().stream().mapToLong(RaftGroup::getOwnershipEpoch).max().orElse(0) + 1;
            for (String groupId : groups.keySet()) {
                RaftGroup group = groups.get(groupId);
                while (group.getOwnershipEpoch() < targetEpoch)
                    group.advanceOwnershipEpoch(group.getOwnershipEpoch() + 1, List.copyOf(desired));
                if (group.getOwnershipEpoch() != targetEpoch)
                    throw new IOException("Ownership epochs diverged during rebalance");
            }
            groups.forEach((id, group) -> ownershipEpochs.put(id, group.getOwnershipEpoch()));

            ring = next;
            routingPublished = true;
            IOException deletionFailure = null;
            for (Move move : copied) {
                try { fenced(move.source(), () -> groups.get(move.source()).deleteForMigration(move.key())); }
                catch (IOException e) {
                    if (deletionFailure == null) deletionFailure = new IOException("New routing is active, but stale source cleanup failed");
                    deletionFailure.addSuppressed(e);
                }
            }
            if (deletionFailure != null) throw deletionFailure;
        } catch (IOException | RuntimeException failure) {
            if (!routingPublished && !ownershipTransitionStarted) {
                for (Move move : copied) {
                    try { groups.get(move.destination()).deleteForMigration(move.key()); }
                    catch (IOException rollbackFailure) { failure.addSuppressed(rollbackFailure); }
                }
            }
            // A failed transition may have advanced only some group logs. Keep each
            // local router aligned with those durable values before releasing freezes.
            groups.forEach((id, group) -> ownershipEpochs.put(id, group.getOwnershipEpoch()));
            if (!ownershipTransitionStarted) for (RaftGroup group : frozen) {
                try { group.setMaintenanceFrozen(false); }
                catch (IOException e) { failure.addSuppressed(e); }
            }
            throw failure;
        } finally {
            routingLock.writeLock().unlock();
        }
        IOException unfreezeFailure = null;
        for (RaftGroup group : frozen) {
            try { group.setMaintenanceFrozen(false); }
            catch (IOException e) { if (unfreezeFailure == null) unfreezeFailure = new IOException("Could not release every Raft shard after rebalance"); unfreezeFailure.addSuppressed(e); }
        }
        if (unfreezeFailure != null) throw unfreezeFailure;
    }

    private record Move(String source, String destination, String key) { }

    private <T> T fenced(String groupId, RaftGroup.IoAction<T> action) throws IOException {
        return groups.get(groupId).withOwnershipEpoch(ownershipEpochs.get(groupId), action);
    }

    private List<String> ringNodes() {
        // Preserve deterministic ordering from the configured group map.
        List<String> result = new java.util.ArrayList<>();
        for (String id : groups.keySet()) if (ring.containsNode(id)) result.add(id);
        return result;
    }

    private static void unfreezeAll(List<RaftGroup> groups, Exception primary) {
        for (RaftGroup group : groups) try { group.setMaintenanceFrozen(false); }
        catch (IOException e) { primary.addSuppressed(e); }
    }

    /** Resolves every durable coordinator record before returning reads to clients. */
    public void recoverPendingTransactions() throws IOException {
        routingLock.writeLock().lock();
        try { recoverTransactionsLocked(true); }
        finally { routingLock.writeLock().unlock(); }
    }

    private void recoverTransactionsLocked(boolean abortUndecided) throws IOException {
        // Coordinator decisions may be replicated to a group whose local intent is
        // already released. Always scan: another participant can still hold a lock.
        Map<String, List<RaftGroup.TransactionRecord>> recordsByGroup = new LinkedHashMap<>();
        for (Map.Entry<String, RaftGroup> group : groups.entrySet())
            recordsByGroup.put(group.getKey(), retryTransactionRead(group.getValue()));
        for (Map.Entry<String, List<RaftGroup.TransactionRecord>> groupRecords : recordsByGroup.entrySet()) {
            for (RaftGroup.TransactionRecord coordinator : groupRecords.getValue()) {
                if (coordinator.participants().isEmpty()
                        || !coordinator.participants().get(0).equals(groupRecords.getKey())) continue;
                Boolean decision = coordinator.decision();
                if (decision == null) {
                    if (!abortUndecided) continue;
                    long currentTerm = retryCurrentTerm(groups.get(groupRecords.getKey()));
                    if (currentTerm <= coordinator.coordinatorTerm()) continue;
                    groups.get(groupRecords.getKey()).decideTransaction(coordinator.id(), coordinator.digest(),
                            coordinator.participants(), false);
                    decision = false;
                }
                for (String participant : coordinator.participants()) {
                    RaftGroup group = groups.get(participant);
                    if (group == null) throw new IOException("Transaction participant is not provisioned: " + participant);
                    RaftGroup.TransactionRecord local = findTransaction(
                            recordsByGroup.getOrDefault(participant, List.of()), coordinator.id());
                    if (decision) {
                        if (local == null || (!local.prepared() && !local.applied()))
                            throw new IOException("Committed transaction is missing participant intent: "
                                    + coordinator.id() + "/" + participant);
                        if (!local.applied()) group.applyCommittedTransaction(coordinator.id(), coordinator.digest());
                    } else if (local != null && local.prepared()) {
                        group.abortPreparedTransaction(coordinator.id(), coordinator.digest());
                    }
                }
            }
        }
    }

    private List<RaftGroup.TransactionRecord> retryTransactionRead(RaftGroup group) throws IOException {
        IOException last = null;
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(20);
        while (System.nanoTime() < deadline) {
            try { return group.transactionsWithReadIndex(); }
            catch (IOException e) { last = e; try { Thread.sleep(50); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException("Interrupted waiting for Raft transaction state", interrupted); }
            }
        }
        throw last == null ? new IOException("Could not read Raft transaction state") : last;
    }

    private long retryCurrentTerm(RaftGroup group) throws IOException {
        IOException last = null;
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(20);
        while (System.nanoTime() < deadline) {
            try { return group.currentTermWithReadIndex(); }
            catch (IOException e) { last = e; try { Thread.sleep(50); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException("Interrupted waiting for coordinator term", interrupted); }
            }
        }
        throw last == null ? new IOException("Could not read coordinator term") : last;
    }

    private static RaftGroup.TransactionRecord findTransaction(List<RaftGroup.TransactionRecord> records, String id) {
        for (RaftGroup.TransactionRecord record : records) if (record.id().equals(id)) return record;
        return null;
    }

    private void executeDistributedTransaction(Map<String, Map<String, String>> changesByGroup) throws IOException {
        List<String> participants = changesByGroup.keySet().stream().sorted().toList();
        if (participants.size() < 2) throw new IllegalArgumentException("Distributed transaction requires at least two groups");
        String coordinatorId = participants.get(0);
        String id = UUID.randomUUID().toString();
        String digest = transactionDigest(changesByGroup);
        RaftGroup coordinator = groups.get(coordinatorId);
            fenced(coordinatorId, () -> { coordinator.beginTransaction(id, digest, participants); return null; });
        List<String> prepared = new java.util.ArrayList<>();
        try {
            for (String participant : participants) {
                fenced(participant, () -> { groups.get(participant).prepareTransaction(id, digest, participants,
                        changesByGroup.get(participant)); return null; });
                prepared.add(participant);
            }
        } catch (IOException prepareFailure) {
            try { fenced(coordinatorId, () -> { coordinator.decideTransaction(id, digest, participants, false); return null; }); }
            catch (IOException decisionFailure) { prepareFailure.addSuppressed(decisionFailure); throw prepareFailure; }
            for (String participant : prepared) try {
                fenced(participant, () -> { groups.get(participant).abortPreparedTransaction(id, digest); return null; });
            } catch (IOException releaseFailure) { prepareFailure.addSuppressed(releaseFailure); }
            throw prepareFailure;
        }
        fenced(coordinatorId, () -> { coordinator.decideTransaction(id, digest, participants, true); return null; });
        for (String participant : participants)
            fenced(participant, () -> { groups.get(participant).applyCommittedTransaction(id, digest); return null; });
    }

    private static String transactionDigest(Map<String, Map<String, String>> changesByGroup) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (String group : changesByGroup.keySet().stream().sorted().toList()) {
                updateDigest(digest, group);
                Map<String, String> changes = changesByGroup.get(group);
                for (String key : changes.keySet().stream().sorted().toList()) {
                    updateDigest(digest, key); String value = changes.get(key);
                    digest.update((byte) (value == null ? 0 : 1)); if (value != null) updateDigest(digest, value);
                }
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException impossible) { throw new IOException("SHA-256 is unavailable", impossible); }
    }

    private static void updateDigest(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(java.nio.ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }

    @Override public String get(String key) {
        routingLock.readLock().lock();
        try {
        try { recoverTransactionsLocked(false); }
        catch (IOException e) { throw new UncheckedIOException("Transaction recovery failed", e); }
        RaftGroup group = groups.get(groupFor(key));
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(20);
        while (true) try {
            byte[] value = fenced(groupFor(key), () -> followerReads && !group.isLeader()
                    ? group.getBoundedStalenessRead(key, maxFollowerCommitLag) : group.getWithReadIndex(key));
            return value == null ? null : new String(value, StandardCharsets.UTF_8);
        } catch (IOException e) {
            if (System.nanoTime() >= deadline || e.getMessage() == null
                    || !e.getMessage().contains("prepared transaction"))
                throw new UncheckedIOException("Raft read failed", e);
            try { recoverTransactionsLocked(false); Thread.sleep(25); }
            catch (IOException recoveryFailure) { e.addSuppressed(recoveryFailure); }
            catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new UncheckedIOException("Interrupted waiting for transaction resolution", new IOException(interrupted));
            }
        }
        } finally { routingLock.readLock().unlock(); }
    }

    @Override public void set(String key, String value) {
        routingLock.readLock().lock(); try {
        try { fenced(groupFor(key), () -> groups.get(groupFor(key)).put(key, value.getBytes(StandardCharsets.UTF_8))); }
        catch (IOException e) { throw new UncheckedIOException("Raft write failed", e); }
        } finally { routingLock.readLock().unlock(); }
    }

    @Override public void set(String key, String value, long expiryMillis) {
        routingLock.readLock().lock(); try {
        try { fenced(groupFor(key), () -> groups.get(groupFor(key)).setWithExpiry(key, value.getBytes(StandardCharsets.UTF_8), expiryMillis)); }
        catch (IOException e) { throw new UncheckedIOException("Raft expiring write failed", e); }
        } finally { routingLock.readLock().unlock(); }
    }

    @Override public void setBatch(Map<String, String> values) {
        routingLock.readLock().lock(); try {
        Map<String, Map<String, byte[]>> batches = new LinkedHashMap<>();
        values.forEach((key, value) -> batches.computeIfAbsent(groupFor(key), ignored -> new LinkedHashMap<>())
                .put(key, value.getBytes(StandardCharsets.UTF_8)));
        try { recoverTransactionsLocked(false); }
        catch (IOException e) { throw new UncheckedIOException("Transaction recovery failed", e); }
        if (batches.size() > 1) {
            Map<String, Map<String, String>> changes = new LinkedHashMap<>();
            batches.forEach((group, batch) -> {
                Map<String, String> valuesByKey = new LinkedHashMap<>();
                batch.forEach((key, value) -> valuesByKey.put(key, new String(value, StandardCharsets.UTF_8)));
                changes.put(group, valuesByKey);
            });
            try { executeDistributedTransaction(changes); }
            catch (IOException e) { throw new UncheckedIOException("Cross-group batch transaction failed", e); }
            return;
        }
        for (Map.Entry<String, Map<String, byte[]>> batch : batches.entrySet()) {
            try { fenced(batch.getKey(), () -> groups.get(batch.getKey()).putBatch(batch.getValue())); }
            catch (IOException e) { throw new UncheckedIOException("Raft batch failed for group " + batch.getKey(), e); }
        }
        } finally { routingLock.readLock().unlock(); }
    }

    @Override public int delete(String... keys) {
        routingLock.readLock().lock(); try {
        try { recoverTransactionsLocked(false); }
        catch (IOException e) { throw new UncheckedIOException("Transaction recovery failed", e); }
        Map<String, Map<String, String>> changes = new LinkedHashMap<>();
        java.util.LinkedHashSet<String> owners = new java.util.LinkedHashSet<>();
        for (String key : keys) {
            String owner = groupFor(key); owners.add(owner);
            changes.computeIfAbsent(owner, ignored -> new LinkedHashMap<>()).put(key, null);
        }
        if (owners.size() > 1) {
            int existed = 0;
            for (String key : new java.util.LinkedHashSet<>(java.util.Arrays.asList(keys)))
                if (getWithoutRecovery(key) != null) existed++;
            try { executeDistributedTransaction(changes); }
            catch (IOException e) { throw new UncheckedIOException("Cross-group delete transaction failed", e); }
            return existed;
        }
        if (keys.length == 0) return 0;
        try { String owner = owners.iterator().next(); return fenced(owner, () -> groups.get(owner).deleteBatch(keys)); }
        catch (IOException e) { throw new UncheckedIOException("Raft delete failed", e); }
        } finally { routingLock.readLock().unlock(); }
    }

    private String getWithoutRecovery(String key) {
        RaftGroup group = groups.get(groupFor(key));
        try {
            byte[] value = fenced(group.getGroupId(), () -> followerReads && !group.isLeader()
                    ? group.getBoundedStalenessRead(key, maxFollowerCommitLag) : group.getWithReadIndex(key));
            return value == null ? null : new String(value, StandardCharsets.UTF_8);
        } catch (IOException e) { throw new UncheckedIOException("Raft read failed", e); }
    }

    @Override public List<String> keys(String pattern) {
        routingLock.readLock().lock(); try {
        try { recoverTransactionsLocked(false); }
        catch (IOException e) { throw new UncheckedIOException("Transaction recovery failed", e); }
        TreeSet<String> result = new TreeSet<>();
        for (RaftGroup group : groups.values()) {
            try {
                result.addAll(fenced(group.getGroupId(), () -> followerReads && !group.isLeader()
                        ? group.keysWithBoundedStaleness(pattern, maxFollowerCommitLag) : group.keysWithReadIndex(pattern)));
            } catch (IOException e) { throw new UncheckedIOException("Raft key scan failed", e); }
        }
        return List.copyOf(result);
        } finally { routingLock.readLock().unlock(); }
    }

    @Override public int size() { return keys("*").size(); }

    @Override public void close() {
        RuntimeException failure = null;
        for (RaftGroup group : groups.values()) {
            try { group.close(); }
            catch (IOException e) {
                if (failure == null) failure = new IllegalStateException("Could not close every Raft group");
                failure.addSuppressed(e);
            }
        }
        if (failure != null) throw failure;
    }
}
