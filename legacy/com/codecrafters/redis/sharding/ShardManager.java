package com.distributedkv.storage.sharding;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

import com.distributedkv.common.ConsistentHash;
import com.distributedkv.config.ClusterConfig;

/**
 * Manages shards in the distributed key-value store.
 *
 * The ShardManager is responsible for creating, assigning, and managing shards
 * based on the cluster configuration. It uses consistent hashing to distribute
 * shards evenly across nodes.
 */
public class ShardManager {
    private static final Logger LOGGER = Logger.getLogger(ShardManager.class.getName());

    /** The cluster configuration */
    private final ClusterConfig clusterConfig;

    /** Map of shards by shard ID */
    private final Map<String, Shard> shards = new ConcurrentHashMap<>();

    /** Map of node IDs to the shard IDs assigned to that node */
    private final Map<String, Set<String>> nodeShards = new ConcurrentHashMap<>();

    /** Lock for shard operations */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** Consistent hash for shard distribution */
    private final ConsistentHash<String> consistentHash;

    /** Data migrator for shard migrations */
    private final DataMigrator dataMigrator;

    /**
     * Creates a new shard manager.
     *
     * @param clusterConfig The cluster configuration
     * @param dataMigrator The data migrator
     */
    public ShardManager(ClusterConfig clusterConfig, DataMigrator dataMigrator) {
        this.clusterConfig = clusterConfig;
        this.dataMigrator = dataMigrator;
        this.consistentHash = new ConsistentHash<>(clusterConfig.getNumVirtualNodes());

        // Add nodes to the consistent hash
        for (String nodeId : clusterConfig.getNodeIds()) {
            consistentHash.add(nodeId);
        }
    }

    /**
     * Initializes the shard manager.
     */
    public void initialize() {
        LOGGER.info("Initializing shard manager");

        lock.writeLock().lock();
        try {
            // Create initial shards if none exist
            if (shards.isEmpty()) {
                createInitialShards();
            }

            // Initialize node shards
            for (String nodeId : clusterConfig.getNodeIds()) {
                if (!nodeShards.containsKey(nodeId)) {
                    nodeShards.put(nodeId, new HashSet<>());
                }
            }

            // Assign shards to nodes
            for (Shard shard : shards.values()) {
                for (String nodeId : shard.getReplicaNodeIds()) {
                    nodeShards.computeIfAbsent(nodeId, k -> new HashSet<>()).add(shard.getShardId());
                }
            }

            LOGGER.info("Shard manager initialized with " + shards.size() + " shards");
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Creates the initial shards based on the cluster configuration.
     */
    private void createInitialShards() {
        int numShards = clusterConfig.getNumShards();
        int replicationFactor = clusterConfig.getReplicationFactor();

        LOGGER.info("Creating " + numShards + " initial shards with replication factor " + replicationFactor);

        // Create shards with even key ranges
        for (int i = 0; i < numShards; i++) {
            String shardId = "shard-" + i;

            // Determine key range for this shard
            byte[] startKey = null;
            byte[] endKey = null;

            if (numShards > 1) {
                // Distribute keys evenly
                if (i > 0) {
                    startKey = generateKeyForShard(i, numShards);
                }

                if (i < numShards - 1) {
                    endKey = generateKeyForShard(i + 1, numShards);
                }
            }

            // Select nodes for this shard using consistent hashing
            List<String> nodeIds = selectNodesForShard(shardId, replicationFactor);

            // Primary is the first node
            String primaryNodeId = nodeIds.get(0);
            Set<String> replicaNodeIds = new HashSet<>(nodeIds);

            // Create the shard
            Shard shard = new Shard(shardId, startKey, endKey, primaryNodeId, replicaNodeIds);
            shards.put(shardId, shard);

            LOGGER.info("Created shard " + shardId + " with primary " + primaryNodeId +
                      " and " + replicaNodeIds.size() + " replicas");
        }
    }

    /**
     * Generates a key that marks the boundary between shards.
     *
     * @param shardIndex The shard index
     * @param totalShards The total number of shards
     * @return The key
     */
    private byte[] generateKeyForShard(int shardIndex, int totalShards) {
        // Use simple string keys for shard boundaries
        String key = "shard-boundary-" + shardIndex + "-of-" + totalShards;
        return key.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Selects nodes for a shard using consistent hashing.
     *
     * @param shardId The shard ID
     * @param replicationFactor The replication factor
     * @return The selected node IDs
     */
    private List<String> selectNodesForShard(String shardId, int replicationFactor) {
        List<String> allNodeIds = new ArrayList<>(clusterConfig.getNodeIds());

        // If we don't have enough nodes for the replication factor,
        // just use all available nodes
        if (allNodeIds.size() <= replicationFactor) {
            return allNodeIds;
        }

        // Use consistent hashing to select the primary node
        String primaryNodeId = consistentHash.get(shardId);

        // Select additional nodes using the closest nodes on the hash ring
        List<String> selectedNodeIds = new ArrayList<>();
        selectedNodeIds.add(primaryNodeId);

        // Get the next closest nodes on the ring
        List<String> nextNodes = consistentHash.getNextN(shardId, replicationFactor * 2);

        for (String nodeId : nextNodes) {
            if (!selectedNodeIds.contains(nodeId)) {
                selectedNodeIds.add(nodeId);

                if (selectedNodeIds.size() >= replicationFactor) {
                    break;
                }
            }
        }

        // If we still don't have enough nodes, add random ones
        if (selectedNodeIds.size() < replicationFactor) {
            Collections.shuffle(allNodeIds);

            for (String nodeId : allNodeIds) {
                if (!selectedNodeIds.contains(nodeId)) {
                    selectedNodeIds.add(nodeId);

                    if (selectedNodeIds.size() >= replicationFactor) {
                        break;
                    }
                }
            }
        }

        return selectedNodeIds;
    }

    /**
     * Gets the shard for a key.
     *
     * @param key The key
     * @return The shard, or null if not found
     */
    public Shard getShardForKey(byte[] key) {
        lock.readLock().lock();
        try {
            for (Shard shard : shards.values()) {
                if (shard.isActive() && shard.containsKey(key)) {
                    return shard;
                }
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Gets a shard by ID.
     *
     * @param shardId The shard ID
     * @return The shard, or null if not found
     */
    public Shard getShard(String shardId) {
        return shards.get(shardId);
    }

    /**
     * Gets all shards.
     *
     * @return Collection of all shards
     */
    public Collection<Shard> getAllShards() {
        return new ArrayList<>(shards.values());
    }

    /**
     * Gets all active shards.
     *
     * @return Collection of active shards
     */
    public Collection<Shard> getActiveShards() {
        List<Shard> activeShards = new ArrayList<>();
        for (Shard shard : shards.values()) {
            if (shard.isActive()) {
                activeShards.add(shard);
            }
        }
        return activeShards;
    }

    /**
     * Gets the shards assigned to a node.
     *
     * @param nodeId The node ID
     * @return Set of shards assigned to the node
     */
    public Set<Shard> getNodeShards(String nodeId) {
        Set<String> shardIds = nodeShards.getOrDefault(nodeId, Collections.emptySet());
        Set<Shard> nodeShards = new HashSet<>();

        for (String shardId : shardIds) {
            Shard shard = shards.get(shardId);
            if (shard != null) {
                nodeShards.add(shard);
            }
        }

        return nodeShards;
    }

    /**
     * Gets the primary shards assigned to a node.
     *
     * @param nodeId The node ID
     * @return Set of primary shards assigned to the node
     */
    public Set<Shard> getNodePrimaryShards(String nodeId) {
        Set<Shard> primaryShards = new HashSet<>();

        for (Shard shard : shards.values()) {
            if (shard.isActive() && nodeId.equals(shard.getPrimaryNodeId())) {
                primaryShards.add(shard);
            }
        }

        return primaryShards;
    }

    /**
     * Gets the replica shards assigned to a node.
     *
     * @param nodeId The node ID
     * @return Set of replica shards assigned to the node
     */
    public Set<Shard> getNodeReplicaShards(String nodeId) {
        Set<Shard> replicaShards = new HashSet<>();

        for (Shard shard : shards.values()) {
            if (shard.isActive() && shard.isReplicaNode(nodeId) && !nodeId.equals(shard.getPrimaryNodeId())) {
                replicaShards.add(shard);
            }
        }

        return replicaShards;
    }

    /**
     * Creates a new shard.
     *
     * @param shardId The shard ID
     * @param startKey The start key
     * @param endKey The end key
     * @param replicationFactor The replication factor
     * @return The created shard
     */
    public Shard createShard(String shardId, byte[] startKey, byte[] endKey, int replicationFactor) {
        lock.writeLock().lock();
        try {
            if (shards.containsKey(shardId)) {
                throw new IllegalArgumentException("Shard already exists: " + shardId);
            }

            // Select nodes for this shard
            List<String> nodeIds = selectNodesForShard(shardId, replicationFactor);

            // Primary is the first node
            String primaryNodeId = nodeIds.get(0);
            Set<String> replicaNodeIds = new HashSet<>(nodeIds);

            // Create the shard
            Shard shard = new Shard(shardId, startKey, endKey, primaryNodeId, replicaNodeIds);
            shards.put(shardId, shard);

            // Update node shards
            for (String nodeId : replicaNodeIds) {
                nodeShards.computeIfAbsent(nodeId, k -> new HashSet<>()).add(shardId);
            }

            LOGGER.info("Created shard " + shardId + " with primary " + primaryNodeId +
                      " and " + replicaNodeIds.size() + " replicas");

            return shard;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Adds a replica to a shard.
     *
     * @param shardId The shard ID
     * @param nodeId The node ID to add as a replica
     * @param migrate Whether to migrate data to the new replica
     * @return True if the replica was added, false otherwise
     */
    public boolean addShardReplica(String shardId, String nodeId, boolean migrate) {
        lock.writeLock().lock();
        try {
            Shard shard = shards.get(shardId);
            if (shard == null) {
                return false;
            }

            if (shard.isReplicaNode(nodeId)) {
                return false;
            }

            // Add the replica
            boolean added = shard.addReplicaNode(nodeId);
            if (added) {
                // Update node shards
                nodeShards.computeIfAbsent(nodeId, k -> new HashSet<>()).add(shardId);

                LOGGER.info("Added node " + nodeId + " as replica for shard " + shardId);

                // Migrate data if requested
                if (migrate && dataMigrator != null) {
                    String sourceNodeId = shard.getPrimaryNodeId();
                    dataMigrator.migrateData(shardId, sourceNodeId, nodeId);
                }
            }

            return added;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Removes a replica from a shard.
     *
     * @param shardId The shard ID
     * @param nodeId The node ID to remove as a replica
     * @return True if the replica was removed, false otherwise
     */
    public boolean removeShardReplica(String shardId, String nodeId) {
        lock.writeLock().lock();
        try {
            Shard shard = shards.get(shardId);
            if (shard == null) {
                return false;
            }

            // Cannot remove the primary
            if (nodeId.equals(shard.getPrimaryNodeId())) {
                return false;
            }

            // Remove the replica
            boolean removed = shard.removeReplicaNode(nodeId);
            if (removed) {
                // Update node shards
                Set<String> nodeShardsSet = nodeShards.get(nodeId);
                if (nodeShardsSet != null) {
                    nodeShardsSet.remove(shardId);
                }

                LOGGER.info("Removed node " + nodeId + " as replica for shard " + shardId);
            }

            return removed;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Sets the primary node for a shard.
     *
     * @param shardId The shard ID
     * @param nodeId The node ID to set as primary
     * @return True if the primary was set, false otherwise
     */
    public boolean setShardPrimary(String shardId, String nodeId) {
        lock.writeLock().lock();
        try {
            Shard shard = shards.get(shardId);
            if (shard == null) {
                return false;
            }

            // Node must be a replica
            if (!shard.isReplicaNode(nodeId)) {
                return false;
            }

            // Set the primary
            String oldPrimaryId = shard.getPrimaryNodeId();
            shard.setPrimaryNodeId(nodeId);

            LOGGER.info("Changed primary for shard " + shardId + " from " +
                      oldPrimaryId + " to " + nodeId);

            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Activates a shard.
     *
     * @param shardId The shard ID
     * @return True if the shard was activated, false otherwise
     */
    public boolean activateShard(String shardId) {
        lock.writeLock().lock();
        try {
            Shard shard = shards.get(shardId);
            if (shard == null) {
                return false;
            }

            if (shard.isActive()) {
                return false;
            }

            shard.setActive(true);

            LOGGER.info("Activated shard " + shardId);

            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Deactivates a shard.
     *
     * @param shardId The shard ID
     * @return True if the shard was deactivated, false otherwise
     */
    public boolean deactivateShard(String shardId) {
        lock.writeLock().lock();
        try {
            Shard shard = shards.get(shardId);
            if (shard == null) {
                return false;
            }

            if (!shard.isActive()) {
                return false;
            }

            shard.setActive(false);

            LOGGER.info("Deactivated shard " + shardId);

            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Removes a shard.
     *
     * @param shardId The shard ID
     * @return True if the shard was removed, false otherwise
     */
    public boolean removeShard(String shardId) {
        lock.writeLock().lock();
        try {
            Shard shard = shards.remove(shardId);
            if (shard == null) {
                return false;
            }

            // Update node shards
            for (String nodeId : shard.getReplicaNodeIds()) {
                Set<String> nodeShardsSet = nodeShards.get(nodeId);
                if (nodeShardsSet != null) {
                    nodeShardsSet.remove(shardId);
                }
            }

            LOGGER.info("Removed shard " + shardId);

            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Rebalances shards across nodes.
     *
     * @param migrate Whether to migrate data during rebalancing
     * @return The number of shards that were rebalanced
     */
    public int rebalanceShards(boolean migrate) {
        lock.writeLock().lock();
        try {
            int rebalanced = 0;

            // TODO: Implement shard rebalancing

            return rebalanced;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Checks if a node is the primary for any shard.
     *
     * @param nodeId The node ID
     * @return True if the node is the primary for any shard
     */
    public boolean isNodePrimary(String nodeId) {
        lock.readLock().lock();
        try {
            for (Shard shard : shards.values()) {
                if (shard.isActive() && nodeId.equals(shard.getPrimaryNodeId())) {
                    return true;
                }
            }
            return false;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Checks if a node is a replica for any shard.
     *
     * @param nodeId The node ID
     * @return True if the node is a replica for any shard
     */
    public boolean isNodeReplica(String nodeId) {
        lock.readLock().lock();
        try {
            for (Shard shard : shards.values()) {
                if (shard.isActive() && shard.isReplicaNode(nodeId)) {
                    return true;
                }
            }
            return false;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Handles a node failure.
     *
     * @param nodeId The failed node ID
     */
    public void handleNodeFailure(String nodeId) {
        lock.writeLock().lock();
        try {
            LOGGER.info("Handling failure of node " + nodeId);

            Set<String> nodeShardsSet = nodeShards.getOrDefault(nodeId, Collections.emptySet());
            Set<String> shardIds = new HashSet<>(nodeShardsSet);

            for (String shardId : shardIds) {
                Shard shard = shards.get(shardId);
                if (shard == null) {
                    continue;
                }

                // If this node was the primary, elect a new primary
                if (nodeId.equals(shard.getPrimaryNodeId())) {
                    electNewPrimary(shard, nodeId);
                }

                // Remove the node from the replica set
                shard.removeReplicaNode(nodeId);

                // Create a new replica if needed
                if (shard.getReplicaCount() < clusterConfig.getReplicationFactor()) {
                    addNewReplica(shard);
                }
            }

            // Remove the node from the node shards map
            nodeShards.remove(nodeId);

            // Remove the node from the consistent hash
            consistentHash.remove(nodeId);

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Elects a new primary for a shard when the current primary fails.
     *
     * @param shard The shard
     * @param failedNodeId The failed node ID
     */
    private void electNewPrimary(Shard shard, String failedNodeId) {
        // Find a new primary among the replicas
        Set<String> replicas = shard.getReplicaNodeIds();
        replicas.remove(failedNodeId);

        if (replicas.isEmpty()) {
            LOGGER.warning("No replicas available for shard " + shard.getShardId() +
                          " after primary failure");
            return;
        }

        // Choose the first available replica as the new primary
        String newPrimaryId = replicas.iterator().next();
        shard.setPrimaryNodeId(newPrimaryId);

        LOGGER.info("Elected new primary " + newPrimaryId + " for shard " +
                  shard.getShardId() + " after failure of " + failedNodeId);
    }

    /**
     * Adds a new replica for a shard when a replica fails.
     *
     * @param shard The shard
     */
    private void addNewReplica(Shard shard) {
        // Find a node that is not already a replica
        List<String> availableNodes = new ArrayList<>(clusterConfig.getNodeIds());
        availableNodes.removeAll(shard.getReplicaNodeIds());

        if (availableNodes.isEmpty()) {
            LOGGER.warning("No available nodes for adding a replica to shard " +
                          shard.getShardId());
            return;
        }

        // Choose a node using consistent hashing
        String shardKey = shard.getShardId() + "-replica-" + System.currentTimeMillis();
        String newNodeId = consistentHash.get(shardKey);

        // If the chosen node is already a replica, pick a random available node
        if (shard.isReplicaNode(newNodeId)) {
            Collections.shuffle(availableNodes);
            newNodeId = availableNodes.get(0);
        }

        // Add the new replica
        boolean added = shard.addReplicaNode(newNodeId);
        if (added) {
            // Update node shards
            nodeShards.computeIfAbsent(newNodeId, k -> new HashSet<>()).add(shard.getShardId());

            LOGGER.info("Added new replica " + newNodeId + " for shard " +
                      shard.getShardId());

            // Migrate data if a data migrator is available
            if (dataMigrator != null) {
                String sourceNodeId = shard.getPrimaryNodeId();
                dataMigrator.migrateData(shard.getShardId(), sourceNodeId, newNodeId);
            }
        }
    }

    /**
     * Gets the number of shards.
     *
     * @return The number of shards
     */
    public int getShardCount() {
        return shards.size();
    }

    /**
     * Gets the number of active shards.
     *
     * @return The number of active shards
     */
    public int getActiveShardCount() {
        int count = 0;
        for (Shard shard : shards.values()) {
            if (shard.isActive()) {
                count++;
            }
        }
        return count;
    }
}