
package com.codecrafters.redis.distributed;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * Consistent hash ring with virtual nodes.
 *
 * This class assigns keys to nodes. It does not perform network
 * communication, replication, or data migration.
 */
public final class ConsistentHashRing {

    private final NavigableMap<Long, String> ring = new TreeMap<>();
    private final int virtualNodes;
    private final MessageDigest digest;

    public ConsistentHashRing(int virtualNodes) {
        if (virtualNodes <= 0) {
            throw new IllegalArgumentException(
                    "virtualNodes must be greater than zero");
        }

        this.virtualNodes = virtualNodes;

        try {
            this.digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(
                    "SHA-256 is unavailable", e);
        }
    }

    public synchronized void addNode(String nodeId) {
        Objects.requireNonNull(nodeId, "nodeId");

        if (nodeId.isBlank()) {
            throw new IllegalArgumentException(
                    "nodeId must not be blank");
        }

        if (containsNode(nodeId)) {
            return;
        }

        for (int i = 0; i < virtualNodes; i++) {
            long hash = hash(nodeId + "#" + i);
            ring.put(hash, nodeId);
        }
    }

    public synchronized void removeNode(String nodeId) {
        if (nodeId == null) {
            return;
        }

        ring.entrySet().removeIf(
                entry -> entry.getValue().equals(nodeId));
    }

    public synchronized String getNode(String key) {
        Objects.requireNonNull(key, "key");

        if (ring.isEmpty()) {
            throw new IllegalStateException(
                    "No nodes are available in the hash ring");
        }

        long keyHash = hash(key);
        var entry = ring.ceilingEntry(keyHash);

        if (entry == null) {
            entry = ring.firstEntry();
        }

        return entry.getValue();
    }

    /**
     * Returns a list of distinct physical nodes for the given key,
     * ordered clockwise starting from the key's position on the ring.
     */
    public synchronized List<String> getPreferenceList(String key, int count) {
        Objects.requireNonNull(key, "key");

        if (count <= 0) {
            throw new IllegalArgumentException("count must be positive");
        }

        if (ring.isEmpty()) {
            throw new IllegalStateException(
                    "No nodes are available in the hash ring");
        }

        long keyHash = hash(key);
        List<String> result = new ArrayList<>();
        Set<String> seen = new HashSet<>();

        var tail = ring.tailMap(keyHash, true);
        for (String node : tail.values()) {
            if (seen.add(node)) {
                result.add(node);
                if (result.size() == count) {
                    return Collections.unmodifiableList(result);
                }
            }
        }

        var head = ring.headMap(keyHash, false);
        for (String node : head.values()) {
            if (seen.add(node)) {
                result.add(node);
                if (result.size() == count) {
                    return Collections.unmodifiableList(result);
                }
            }
        }

        return Collections.unmodifiableList(result);
    }

    public synchronized boolean containsNode(String nodeId) {
        return ring.containsValue(nodeId);
    }

    public synchronized int getNodeCount() {
        return (int) ring.values().stream().distinct().count();
    }

    public synchronized int getVirtualNodeCount() {
        return ring.size();
    }

    public synchronized Collection<String> getNodes() {
        return Collections.unmodifiableSet(
                new java.util.TreeSet<>(ring.values()));
    }

    private long hash(String value) {
        byte[] bytes = digest.digest(
                value.getBytes(StandardCharsets.UTF_8));

        long result = 0;

        for (int i = 0; i < Long.BYTES; i++) {
            result = (result << 8) | (bytes[i] & 0xffL);
        }

        return result & Long.MAX_VALUE;
    }
}
