package com.distributedkv.storage.sharding;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a shard in the distributed key-value store.
 *
 * A shard is a horizontal partition of data in the database. Each shard contains
 * a subset of the data and is replicated across multiple nodes for fault tolerance.
 */
public class Shard {

    /** The unique ID of this shard */
    private final String shardId;

    /** The range start key (inclusive) */
    private final byte[] startKey;

    /** The range end key (exclusive) */
    private final byte[] endKey;

    /** The set of node IDs that host replicas of this shard */
    private final Set<String> replicaNodeIds;

    /** The primary node ID for this shard */
    private String primaryNodeId;

    /** Flag indicating whether this shard is active */
    private boolean active;

    /**
     * Creates a new shard.
     *
     * @param shardId The shard ID
     * @param startKey The start key
     * @param endKey The end key
     * @param primaryNodeId The primary node ID
     * @param replicaNodeIds The replica node IDs
     */
    public Shard(String shardId, byte[] startKey, byte[] endKey, String primaryNodeId, Set<String> replicaNodeIds) {
        this.shardId = shardId;
        this.startKey = startKey;
        this.endKey = endKey;
        this.primaryNodeId = primaryNodeId;
        this.replicaNodeIds = new HashSet<>(replicaNodeIds);
        this.active = true;
    }

    /**
     * Gets the shard ID.
     *
     * @return The shard ID
     */
    public String getShardId() {
        return shardId;
    }

    /**
     * Gets the start key.
     *
     * @return The start key
     */
    public byte[] getStartKey() {
        return startKey;
    }

    /**
     * Gets the end key.
     *
     * @return The end key
     */
    public byte[] getEndKey() {
        return endKey;
    }

    /**
     * Gets the primary node ID.
     *
     * @return The primary node ID
     */
    public String getPrimaryNodeId() {
        return primaryNodeId;
    }

    /**
     * Sets the primary node ID.
     *
     * @param primaryNodeId The primary node ID
     */
    public void setPrimaryNodeId(String primaryNodeId) {
        this.primaryNodeId = primaryNodeId;
    }

    /**
     * Gets the replica node IDs.
     *
     * @return The replica node IDs
     */
    public Set<String> getReplicaNodeIds() {
        return new HashSet<>(replicaNodeIds);
    }

    /**
     * Adds a replica node.
     *
     * @param nodeId The node ID
     * @return True if the node was added, false if it was already a replica
     */
    public boolean addReplicaNode(String nodeId) {
        return replicaNodeIds.add(nodeId);
    }

    /**
     * Removes a replica node.
     *
     * @param nodeId The node ID
     * @return True if the node was removed, false if it was not a replica
     */
    public boolean removeReplicaNode(String nodeId) {
        if (nodeId.equals(primaryNodeId)) {
            return false;
        }
        return replicaNodeIds.remove(nodeId);
    }

    /**
     * Checks if a node is a replica of this shard.
     *
     * @param nodeId The node ID
     * @return True if the node is a replica
     */
    public boolean isReplicaNode(String nodeId) {
        return replicaNodeIds.contains(nodeId);
    }

    /**
     * Gets the number of replicas.
     *
     * @return The number of replicas
     */
    public int getReplicaCount() {
        return replicaNodeIds.size();
    }

    /**
     * Checks if this shard is active.
     *
     * @return True if this shard is active
     */
    public boolean isActive() {
        return active;
    }

    /**
     * Sets whether this shard is active.
     *
     * @param active Whether this shard is active
     */
    public void setActive(boolean active) {
        this.active = active;
    }

    /**
     * Checks if a key belongs to this shard.
     *
     * @param key The key to check
     * @return True if the key belongs to this shard
     */
    public boolean containsKey(byte[] key) {
        if (startKey == null && endKey == null) {
            return true;
        }

        if (startKey != null) {
            if (compareKeys(key, startKey) < 0) {
                return false;
            }
        }

        if (endKey != null) {
            if (compareKeys(key, endKey) >= 0) {
                return false;
            }
        }

        return true;
    }

    /**
     * Compares two byte arrays lexicographically.
     *
     * @param key1 The first key
     * @param key2 The second key
     * @return A negative, zero, or positive integer as the first key is less than, equal to,
     *         or greater than the second key
     */
    private int compareKeys(byte[] key1, byte[] key2) {
        int len1 = key1.length;
        int len2 = key2.length;
        int lim = Math.min(len1, len2);

        for (int i = 0; i < lim; i++) {
            int a = key1[i] & 0xff;
            int b = key2[i] & 0xff;
            if (a != b) {
                return a - b;
            }
        }

        return len1 - len2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Shard shard = (Shard) o;
        return Objects.equals(shardId, shard.shardId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardId);
    }

    @Override
    public String toString() {
        return "Shard{" +
                "shardId='" + shardId + '\'' +
                ", primaryNodeId='" + primaryNodeId + '\'' +
                ", replicaCount=" + replicaNodeIds.size() +
                ", active=" + active +
                '}';
    }
}