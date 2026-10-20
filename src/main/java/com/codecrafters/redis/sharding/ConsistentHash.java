package com.codecrafters.redis.sharding;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Thread-safe consistent hash ring with deterministic virtual nodes. */
public final class ConsistentHash<T> {
    private final int virtualNodesPerNode;
    private final NavigableMap<Long, T> ring = new TreeMap<>();
    private final Map<T, List<Long>> positions = new java.util.HashMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public ConsistentHash() { this(128); }

    public ConsistentHash(int virtualNodesPerNode) {
        if (virtualNodesPerNode <= 0) throw new IllegalArgumentException("virtual node count must be positive");
        this.virtualNodesPerNode = virtualNodesPerNode;
    }

    public void addNode(T node) {
        Objects.requireNonNull(node, "node");
        lock.writeLock().lock();
        try {
            if (positions.containsKey(node)) return;
            List<Long> nodePositions = new ArrayList<>(virtualNodesPerNode);
            for (int i = 0; i < virtualNodesPerNode; i++) {
                long point = hash(node.toString() + "#" + i);
                while (ring.containsKey(point)) point++;
                ring.put(point, node);
                nodePositions.add(point);
            }
            positions.put(node, nodePositions);
        } finally { lock.writeLock().unlock(); }
    }

    public void addNodes(Collection<T> nodes) {
        if (nodes == null) throw new IllegalArgumentException("nodes cannot be null");
        for (T node : nodes) addNode(node);
    }

    public void removeNode(T node) {
        if (node == null) throw new IllegalArgumentException("node cannot be null");
        lock.writeLock().lock();
        try {
            List<Long> nodePositions = positions.remove(node);
            if (nodePositions != null) nodePositions.forEach(ring::remove);
        } finally { lock.writeLock().unlock(); }
    }

    public T getNode(String key) {
        Objects.requireNonNull(key, "key");
        lock.readLock().lock();
        try {
            if (ring.isEmpty()) return null;
            Map.Entry<Long, T> entry = ring.ceilingEntry(hash(key));
            return (entry != null ? entry : ring.firstEntry()).getValue();
        } finally { lock.readLock().unlock(); }
    }

    public List<T> getNodes(String key, int count) {
        Objects.requireNonNull(key, "key");
        if (count <= 0) throw new IllegalArgumentException("count must be positive");
        lock.readLock().lock();
        try {
            if (ring.isEmpty()) return List.of();
            LinkedHashSet<T> result = new LinkedHashSet<>();
            long point = hash(key);
            for (Map.Entry<Long, T> entry : ring.tailMap(point, true).entrySet()) {
                result.add(entry.getValue());
                if (result.size() == count) return List.copyOf(result);
            }
            for (Map.Entry<Long, T> entry : ring.headMap(point, false).entrySet()) {
                result.add(entry.getValue());
                if (result.size() == count) break;
            }
            return List.copyOf(result);
        } finally { lock.readLock().unlock(); }
    }

    public int size() {
        lock.readLock().lock();
        try { return positions.size(); }
        finally { lock.readLock().unlock(); }
    }

    public boolean containsNode(T node) {
        lock.readLock().lock();
        try { return positions.containsKey(node); }
        finally { lock.readLock().unlock(); }
    }

    private static long hash(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
            return ByteBuffer.wrap(digest).getLong();
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError("SHA-256 is required by the Java platform", impossible);
        }
    }
}
