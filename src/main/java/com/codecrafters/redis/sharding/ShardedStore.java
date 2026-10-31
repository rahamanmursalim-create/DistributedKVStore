package com.codecrafters.redis.sharding;

import com.codecrafters.redis.storage.RedisStore;
import com.codecrafters.redis.storage.KeyValueAccess;
import com.codecrafters.redis.storage.ValueEntry;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Routes key operations over stores and synchronously rebalances membership changes. */
public final class ShardedStore implements KeyValueAccess, AutoCloseable {
    private record Copy(String owner, String key, ValueEntry value) {}
    private record Removal(RedisStore store, String key) {}
    private final int virtualNodesPerNode;
    private final Map<String, RedisStore> stores = new LinkedHashMap<>();
    private volatile ConsistentHash<String> ring;
    private final ReentrantReadWriteLock membershipLock = new ReentrantReadWriteLock(true);

    public ShardedStore() { this(128); }
    public ShardedStore(int virtualNodesPerNode) {
        if (virtualNodesPerNode <= 0) throw new IllegalArgumentException("virtual node count must be positive");
        this.virtualNodesPerNode = virtualNodesPerNode;
        this.ring = new ConsistentHash<>(virtualNodesPerNode);
    }

    /** Copies moved keys before publishing the new ring; persisted joining stores are reconciled. */
    public void addNode(String nodeId, RedisStore store) {
        addNode(nodeId, store, () -> { });
    }

    /** Adds a member after copying data and durably recording membership, before publishing routing. */
    void addNode(String nodeId, RedisStore store, Runnable persistMembership) {
        requireNode(nodeId); Objects.requireNonNull(store, "store");
        Objects.requireNonNull(persistMembership, "persistMembership");
        membershipLock.writeLock().lock();
        try {
            if (stores.containsKey(nodeId)) throw new IllegalArgumentException("Node already exists: " + nodeId);
            if (stores.values().stream().anyMatch(existing -> existing == store))
                throw new IllegalArgumentException("A RedisStore cannot be registered under multiple node IDs");
            ConsistentHash<String> nextRing = ringWith(nodeId, false);
            List<Removal> removals = copyChangedKeys(ring, nextRing, stores, nodeId, store);
            try { persistMembership.run(); }
            catch (RuntimeException failure) {
                rollbackCopies(removals, failure);
                throw failure;
            }
            stores.put(nodeId, store); ring = nextRing; deleteOldCopies(removals);
        } finally { membershipLock.writeLock().unlock(); }
    }

    /** Copies owned keys before removing a member from routing. */
    public void removeNode(String nodeId) {
        removeNode(nodeId, () -> { });
    }

    /** Removes a member only after its replacement data and membership are durable. */
    void removeNode(String nodeId, Runnable persistMembership) {
        requireNode(nodeId);
        Objects.requireNonNull(persistMembership, "persistMembership");
        membershipLock.writeLock().lock();
        try {
            RedisStore removed = stores.get(nodeId);
            if (removed == null) throw new IllegalArgumentException("Unknown node: " + nodeId);
            if (stores.size() == 1) throw new IllegalStateException("Cannot remove the last storage node");
            ConsistentHash<String> nextRing = ringWith(nodeId, true);
            Map<String, RedisStore> remaining = new LinkedHashMap<>(stores); remaining.remove(nodeId);
            List<Removal> removals = copyChangedKeys(ring, nextRing, remaining, nodeId, removed);
            try { persistMembership.run(); }
            catch (RuntimeException failure) {
                rollbackCopies(removals, failure);
                throw failure;
            }
            ring = nextRing; stores.remove(nodeId); deleteOldCopies(removals);
            // ShardedStore owns registered stores (close() closes all remaining stores),
            // so a successfully removed member must release its WAL/RocksDB resources too.
            removed.close();
        } finally { membershipLock.writeLock().unlock(); }
    }

    public String get(String key) {
        Objects.requireNonNull(key, "key"); membershipLock.readLock().lock();
        try { return ownerStore(ring, key, stores).get(key); }
        finally { membershipLock.readLock().unlock(); }
    }
    public void set(String key, String value) {
        Objects.requireNonNull(key, "key"); Objects.requireNonNull(value, "value"); membershipLock.readLock().lock();
        try { ownerStore(ring, key, stores).set(key, value); }
        finally { membershipLock.readLock().unlock(); }
    }
    public boolean delete(String key) {
        Objects.requireNonNull(key, "key"); membershipLock.readLock().lock();
        try { return ownerStore(ring, key, stores).del(key) > 0; }
        finally { membershipLock.readLock().unlock(); }
    }
    @Override public int delete(String... keys) {
        int deleted = 0;
        for (String key : keys) if (delete(key)) deleted++;
        return deleted;
    }
    @Override public void set(String key, String value, long expiryMillis) {
        Objects.requireNonNull(key, "key"); Objects.requireNonNull(value, "value");
        membershipLock.readLock().lock();
        try { ownerStore(ring, key, stores).set(key, value, expiryMillis); }
        finally { membershipLock.readLock().unlock(); }
    }
    @Override public void setBatch(Map<String, String> values) {
        Objects.requireNonNull(values, "values");
        membershipLock.readLock().lock();
        try {
            Map<RedisStore, Map<String, String>> batches = new LinkedHashMap<>();
            for (Map.Entry<String, String> entry : values.entrySet()) {
                if (entry.getKey() == null || entry.getValue() == null)
                    throw new IllegalArgumentException("Key and value cannot be null");
                RedisStore owner = ownerStore(ring, entry.getKey(), stores);
                batches.computeIfAbsent(owner, ignored -> new LinkedHashMap<>())
                        .put(entry.getKey(), entry.getValue());
            }
            batches.forEach(RedisStore::setBatch);
        } finally { membershipLock.readLock().unlock(); }
    }
    @Override public List<String> keys(String pattern) {
        membershipLock.readLock().lock();
        try {
            java.util.TreeSet<String> keys = new java.util.TreeSet<>();
            for (RedisStore store : stores.values()) keys.addAll(store.keys(pattern));
            return List.copyOf(keys);
        } finally { membershipLock.readLock().unlock(); }
    }
    @Override public int size() {
        membershipLock.readLock().lock();
        try { int size = 0; for (RedisStore store : stores.values()) size += store.size(); return size; }
        finally { membershipLock.readLock().unlock(); }
    }
    public String ownerOf(String key) {
        Objects.requireNonNull(key, "key"); membershipLock.readLock().lock();
        try {
            String owner = ring.getNode(key);
            if (owner == null) throw new IllegalStateException("No storage nodes are registered");
            return owner;
        } finally { membershipLock.readLock().unlock(); }
    }
    public List<String> nodeIds() {
        membershipLock.readLock().lock();
        try { return List.copyOf(stores.keySet()); }
        finally { membershipLock.readLock().unlock(); }
    }

    private ConsistentHash<String> ringWith(String nodeId, boolean remove) {
        ConsistentHash<String> next = new ConsistentHash<>(virtualNodesPerNode);
        for (String existing : stores.keySet()) if (!remove || !existing.equals(nodeId)) next.addNode(existing);
        if (!remove) next.addNode(nodeId);
        return next;
    }
    private static List<Removal> copyChangedKeys(ConsistentHash<String> previous, ConsistentHash<String> next,
            Map<String, RedisStore> destinations, String joiningNode, RedisStore joiningStore) {
        Map<String, RedisStore> sources = new LinkedHashMap<>(destinations);
        if (!destinations.containsKey(joiningNode)) sources.put(joiningNode, joiningStore);
        List<Copy> copies = new ArrayList<>(); List<Removal> removals = new ArrayList<>();
        for (Map.Entry<String, RedisStore> source : sources.entrySet()) {
            for (String key : source.getValue().keys("*")) {
                String oldOwner = previous.getNode(key);
                boolean persistedJoiningData = source.getKey().equals(joiningNode);
                if (!persistedJoiningData && !source.getKey().equals(oldOwner)) continue;
                String newOwner = next.getNode(key);
                if (newOwner == null || newOwner.equals(source.getKey())) continue;
                ValueEntry value = source.getValue().getEntry(key);
                if (value == null || value.isExpired()) continue;
                if (!newOwner.equals(joiningNode) && !destinations.containsKey(newOwner))
                    throw new IllegalStateException("Missing destination store " + newOwner);
                copies.add(new Copy(newOwner, key, value)); removals.add(new Removal(source.getValue(), key));
            }
        }
        // All writes must succeed before old membership stops serving requests.
        List<Removal> appliedCopies = new ArrayList<>();
        try {
            for (Copy copy : copies) {
                RedisStore destination = copy.owner().equals(joiningNode) ? joiningStore : destinations.get(copy.owner());
                ValueEntry existing = destination.getEntry(copy.key());
                if (existing != null && (!existing.getValue().equals(copy.value().getValue())
                        || existing.getExpiryTimestampMillis() != copy.value().getExpiryTimestampMillis()))
                    throw new IllegalStateException("Conflicting persisted value for key " + copy.key());
                if (copy.value().hasExpiry()) {
                    destination.setWithAbsoluteExpiry(copy.key(), copy.value().getValue(),
                            copy.value().getExpiryTimestampMillis());
                } else destination.set(copy.key(), copy.value().getValue());
                appliedCopies.add(new Removal(destination, copy.key()));
            }
        } catch (RuntimeException migrationFailure) {
            for (Removal applied : appliedCopies) {
                try { applied.store().del(applied.key()); }
                catch (RuntimeException rollbackFailure) { migrationFailure.addSuppressed(rollbackFailure); }
            }
            throw migrationFailure;
        }
        return removals;
    }
    private static void deleteOldCopies(List<Removal> removals) {
        for (Removal removal : removals) removal.store().del(removal.key());
    }
    private static void rollbackCopies(List<Removal> copies, RuntimeException failure) {
        for (Removal copy : copies) {
            try { copy.store().del(copy.key()); }
            catch (RuntimeException rollbackFailure) { failure.addSuppressed(rollbackFailure); }
        }
    }
    private RedisStore ownerStore(ConsistentHash<String> selectedRing, String key, Map<String, RedisStore> selectedStores) {
        String owner = selectedRing.getNode(key); RedisStore store = owner == null ? null : selectedStores.get(owner);
        if (store == null) throw new IllegalStateException("No storage node owns this key");
        return store;
    }
    private static void requireNode(String nodeId) {
        if (nodeId == null || nodeId.isBlank()) throw new IllegalArgumentException("node ID cannot be blank");
    }

    @Override public void close() {
        membershipLock.writeLock().lock();
        try { stores.values().forEach(RedisStore::close); stores.clear(); }
        finally { membershipLock.writeLock().unlock(); }
    }
}
