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

/** Routes each key to one independently replicated Raft group using a virtual-node hash ring. */
public final class MultiRaftStore implements KeyValueAccess, AutoCloseable {
    private final Map<String, RaftGroup> groups;
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
        if (virtualNodesPerGroup <= 0) throw new IllegalArgumentException("virtual node count must be positive");
        this.virtualNodesPerGroup = virtualNodesPerGroup;
        this.ring = new ConsistentHash<>(virtualNodesPerGroup);
        this.groups.keySet().forEach(ring::addNode);
        this.followerReads = followerReads;
        if (maxFollowerCommitLag < 0) throw new IllegalArgumentException("follower lag bound cannot be negative");
        this.maxFollowerCommitLag = maxFollowerCommitLag;
    }

    public String groupFor(String key) { return ring.getNode(Objects.requireNonNull(key, "key")); }

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
        try {
            java.util.LinkedHashSet<String> desired = new java.util.LinkedHashSet<>(activeGroupIds);
            if (desired.isEmpty() || desired.size() != activeGroupIds.size() || !groups.keySet().containsAll(desired))
                throw new IllegalArgumentException("Active group set must contain unique provisioned groups");
            ConsistentHash<String> next = new ConsistentHash<>(virtualNodesPerGroup);
            desired.forEach(next::addNode);
            if (new java.util.LinkedHashSet<>(ringNodes()).equals(desired)) return;
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
                        groups.get(destination).putEntry(entry.getKey(), entry.getValue());
                        copied.add(new Move(shard.getKey(), destination, entry.getKey()));
                    }
                }
            }

            ring = next;
            routingPublished = true;
            IOException deletionFailure = null;
            for (Move move : copied) {
                try { groups.get(move.source()).deleteForMigration(move.key()); }
                catch (IOException e) {
                    if (deletionFailure == null) deletionFailure = new IOException("New routing is active, but stale source cleanup failed");
                    deletionFailure.addSuppressed(e);
                }
            }
            if (deletionFailure != null) throw deletionFailure;
        } catch (IOException | RuntimeException failure) {
            if (!routingPublished) {
                for (Move move : copied) {
                    try { groups.get(move.destination()).deleteForMigration(move.key()); }
                    catch (IOException rollbackFailure) { failure.addSuppressed(rollbackFailure); }
                }
            }
            for (RaftGroup group : frozen) {
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

    @Override public String get(String key) {
        routingLock.readLock().lock();
        try {
        RaftGroup group = groups.get(groupFor(key));
        try {
            byte[] value = followerReads && !group.isLeader()
                    ? group.getBoundedStalenessRead(key, maxFollowerCommitLag) : group.getWithReadIndex(key);
            return value == null ? null : new String(value, StandardCharsets.UTF_8);
        } catch (IOException e) { throw new UncheckedIOException("Raft read failed", e); }
        } finally { routingLock.readLock().unlock(); }
    }

    @Override public void set(String key, String value) {
        routingLock.readLock().lock(); try {
        try { groups.get(groupFor(key)).put(key, value.getBytes(StandardCharsets.UTF_8)); }
        catch (IOException e) { throw new UncheckedIOException("Raft write failed", e); }
        } finally { routingLock.readLock().unlock(); }
    }

    @Override public void set(String key, String value, long expiryMillis) {
        routingLock.readLock().lock(); try {
        try { groups.get(groupFor(key)).setWithExpiry(key, value.getBytes(StandardCharsets.UTF_8), expiryMillis); }
        catch (IOException e) { throw new UncheckedIOException("Raft expiring write failed", e); }
        } finally { routingLock.readLock().unlock(); }
    }

    @Override public void setBatch(Map<String, String> values) {
        routingLock.readLock().lock(); try {
        Map<String, Map<String, byte[]>> batches = new LinkedHashMap<>();
        values.forEach((key, value) -> batches.computeIfAbsent(groupFor(key), ignored -> new LinkedHashMap<>())
                .put(key, value.getBytes(StandardCharsets.UTF_8)));
        for (Map.Entry<String, Map<String, byte[]>> batch : batches.entrySet()) {
            try { groups.get(batch.getKey()).putBatch(batch.getValue()); }
            catch (IOException e) { throw new UncheckedIOException("Raft batch failed for group " + batch.getKey(), e); }
        }
        } finally { routingLock.readLock().unlock(); }
    }

    @Override public int delete(String... keys) {
        routingLock.readLock().lock(); try {
        int deleted = 0;
        for (String key : keys) {
            try { if (groups.get(groupFor(key)).delete(key)) deleted++; }
            catch (IOException e) { throw new UncheckedIOException("Raft delete failed", e); }
        }
        return deleted;
        } finally { routingLock.readLock().unlock(); }
    }

    @Override public List<String> keys(String pattern) {
        routingLock.readLock().lock(); try {
        TreeSet<String> result = new TreeSet<>();
        for (RaftGroup group : groups.values()) {
            try {
                result.addAll(followerReads && !group.isLeader()
                        ? group.keysWithBoundedStaleness(pattern, maxFollowerCommitLag) : group.keysWithReadIndex(pattern));
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
