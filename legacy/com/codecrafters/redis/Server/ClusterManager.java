package com.distributedkv.server;

import com.distributedkv.common.ConsistentHash;
import com.distributedkv.common.Result;
import com.distributedkv.common.Status;
import com.distributedkv.config.ClusterConfig;
import com.distributedkv.config.NodeConfig;
import com.distributedkv.network.client.RpcClient;
import com.distributedkv.network.protocol.Message;
import com.distributedkv.raft.RaftGroup;
import com.distributedkv.raft.RaftGroupManager;
import com.distributedkv.storage.sharding.Shard;
import com.distributedkv.storage.sharding.ShardManager;
import com.distributedkv.storage.sharding.DataMigrator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

/**
 * ClusterManager is responsible for coordinating the nodes in the cluster,
 * managing shards, and handling the routing of requests to the appropriate nodes.
 * It implements the consistent hashing algorithm to distribute data across shards
 * and handles data migration when nodes join or leave the cluster.
 */
public class ClusterManager {
    private static final Logger LOGGER = Logger.getLogger(ClusterManager.class.getName());

    // Configuration for the cluster
    private final ClusterConfig clusterConfig;

    // Node manager responsible for individual node operations
    private final NodeManager nodeManager;

    // Consistent hashing for distributing data across shards
    private final ConsistentHash<String> consistentHash;

    // Shard manager for managing the shards in the cluster
    private final ShardManager shardManager;

    // Data migrator for handling data migration between shards
    private final DataMigrator dataMigrator;

    // Raft group manager for managing Raft consensus groups
    private final RaftGroupManager raftGroupManager;

    // Mapping of node IDs to RPC clients
    private final Map<String, RpcClient> nodeClients;

    // Mapping of shard IDs to Raft groups
    private final Map<String, RaftGroup> shardToRaftGroup;

    // Read-write lock for cluster state changes
    private final ReadWriteLock clusterLock;

    // Scheduled executor for background tasks
    private final ScheduledExecutorService scheduler;

    // Status of the cluster manager
    private volatile boolean isRunning;

    /**
     * Creates a new cluster manager with the given configuration and node manager.
     *
     * @param clusterConfig The cluster configuration
     * @param nodeManager The node manager
     * @param raftGroupManager The Raft group manager
     * @param shardManager The shard manager
     * @param dataMigrator The data migrator
     */
    public ClusterManager(ClusterConfig clusterConfig, NodeManager nodeManager,
                          RaftGroupManager raftGroupManager, ShardManager shardManager,
                          DataMigrator dataMigrator) {
        this.clusterConfig = clusterConfig;
        this.nodeManager = nodeManager;
        this.raftGroupManager = raftGroupManager;
        this.shardManager = shardManager;
        this.dataMigrator = dataMigrator;

        // Initialize consistent hash with virtual nodes for better distribution
        this.consistentHash = new ConsistentHash<>(
            clusterConfig.getVirtualNodesCount(),
            new ArrayList<>()
        );

        this.nodeClients = new ConcurrentHashMap<>();
        this.shardToRaftGroup = new ConcurrentHashMap<>();
        this.clusterLock = new ReentrantReadWriteLock();
        this.scheduler = new ScheduledThreadPoolExecutor(2);
        this.isRunning = false;
    }

    /**
     * Starts the cluster manager.
     *
     * @return A result indicating success or failure
     */
    public Result<Void> start() {
        clusterLock.writeLock().lock();
        try {
            if (isRunning) {
                return Result.success(null);
            }

            LOGGER.info("Starting cluster manager...");

            // Initialize the node configuration from the cluster configuration
            for (NodeConfig nodeConfig : clusterConfig.getNodes()) {
                String nodeId = nodeConfig.getNodeId();

                // Initialize the RPC client for the node
                RpcClient client = new RpcClient(nodeConfig.getHost(), nodeConfig.getPort());
                Result<Void> result = client.start();
                if (!result.isSuccess()) {
                    LOGGER.severe("Failed to initialize RPC client for node " + nodeId + ": " + result.getStatus());
                    return Result.failure(result.getStatus());
                }
                nodeClients.put(nodeId, client);

                // Add the node to the consistent hash ring
                consistentHash.addNode(nodeId);
            }

            // Initialize shards and Raft groups
            for (int i = 0; i < clusterConfig.getShardCount(); i++) {
                String shardId = "shard-" + i;

                // Initialize the shard
                Shard shard = new Shard(shardId, clusterConfig.getReplicationFactor());
                Result<Void> result = shardManager.addShard(shard);
                if (!result.isSuccess()) {
                    LOGGER.severe("Failed to add shard " + shardId + ": " + result.getStatus());
                    return Result.failure(result.getStatus());
                }

                // Create a Raft group for the shard
                List<String> nodes = getNodesForShard(shardId, clusterConfig.getReplicationFactor());
                RaftGroup raftGroup = raftGroupManager.createGroup(shardId, nodes);
                shardToRaftGroup.put(shardId, raftGroup);

                // Start the Raft group
                result = raftGroup.start();
                if (!result.isSuccess()) {
                    LOGGER.severe("Failed to start Raft group for shard " + shardId + ": " + result.getStatus());
                    return Result.failure(result.getStatus());
                }
            }

            // Schedule health check and rebalancing tasks
            scheduler.scheduleAtFixedRate(this::performHealthCheck,
                clusterConfig.getHealthCheckIntervalMs(),
                clusterConfig.getHealthCheckIntervalMs(),
                TimeUnit.MILLISECONDS);

            scheduler.scheduleAtFixedRate(this::rebalanceShards,
                clusterConfig.getRebalanceIntervalMs(),
                clusterConfig.getRebalanceIntervalMs(),
                TimeUnit.MILLISECONDS);

            isRunning = true;
            LOGGER.info("Cluster manager started successfully");
            return Result.success(null);
        } catch (Exception e) {
            LOGGER.severe("Failed to start cluster manager: " + e.getMessage());
            return Result.failure(Status.INTERNAL_ERROR, e.getMessage());
        } finally {
            clusterLock.writeLock().unlock();
        }
    }

    /**
     * Stops the cluster manager.
     *
     * @return A result indicating success or failure
     */
    public Result<Void> stop() {
        clusterLock.writeLock().lock();
        try {
            if (!isRunning) {
                return Result.success(null);
            }

            LOGGER.info("Stopping cluster manager...");

            // Stop the scheduler
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }

            // Stop all Raft groups
            for (RaftGroup raftGroup : shardToRaftGroup.values()) {
                Result<Void> result = raftGroup.stop();
                if (!result.isSuccess()) {
                    LOGGER.warning("Failed to stop Raft group " + raftGroup.getGroupId() + ": " + result.getStatus());
                }
            }

            // Close all RPC clients
            for (Map.Entry<String, RpcClient> entry : nodeClients.entrySet()) {
                String nodeId = entry.getKey();
                RpcClient client = entry.getValue();
                Result<Void> result = client.stop();
                if (!result.isSuccess()) {
                    LOGGER.warning("Failed to stop RPC client for node " + nodeId + ": " + result.getStatus());
                }
            }

            isRunning = false;
            LOGGER.info("Cluster manager stopped successfully");
            return Result.success(null);
        } catch (Exception e) {
            LOGGER.severe("Failed to stop cluster manager: " + e.getMessage());
            return Result.failure(Status.INTERNAL_ERROR, e.getMessage());
        } finally {
            clusterLock.writeLock().unlock();
        }
    }

    /**
     * Determines the shard for the given key using consistent hashing.
     *
     * @param key The key
     * @return The shard ID
     */
    public String getShardForKey(String key) {
        clusterLock.readLock().lock();
        try {
            // Get the node for the key using consistent hashing
            String nodeId = consistentHash.getNode(key);

            // Find the shard hosted by this node (this is a simplification;
            // in a real system, we would have a more sophisticated mapping)
            for (Map.Entry<String, RaftGroup> entry : shardToRaftGroup.entrySet()) {
                String shardId = entry.getKey();
                RaftGroup raftGroup = entry.getValue();
                if (raftGroup.getMembers().contains(nodeId)) {
                    return shardId;
                }
            }

            // Fallback to the first shard if no match is found
            return shardToRaftGroup.keySet().iterator().next();
        } finally {
            clusterLock.readLock().unlock();
        }
    }

    /**
     * Returns the Raft group for the given shard.
     *
     * @param shardId The shard ID
     * @return The Raft group
     */
    public RaftGroup getRaftGroupForShard(String shardId) {
        clusterLock.readLock().lock();
        try {
            return shardToRaftGroup.get(shardId);
        } finally {
            clusterLock.readLock().unlock();
        }
    }

    /**
     * Adds a node to the cluster.
     *
     * @param nodeConfig The node configuration
     * @return A result indicating success or failure
     */
    public Result<Void> addNode(NodeConfig nodeConfig) {
        clusterLock.writeLock().lock();
        try {
            String nodeId = nodeConfig.getNodeId();
            LOGGER.info("Adding node " + nodeId + " to the cluster");

            // Check if the node already exists
            if (nodeClients.containsKey(nodeId)) {
                return Result.failure(Status.ALREADY_EXISTS, "Node already exists in the cluster");
            }

            // Initialize the RPC client for the node
            RpcClient client = new RpcClient(nodeConfig.getHost(), nodeConfig.getPort());
            Result<Void> result = client.start();
            if (!result.isSuccess()) {
                LOGGER.severe("Failed to initialize RPC client for node " + nodeId + ": " + result.getStatus());
                return Result.failure(result.getStatus());
            }
            nodeClients.put(nodeId, client);

            // Add the node to the consistent hash ring
            consistentHash.addNode(nodeId);

            // Rebalance shards to include the new node
            result = rebalanceShardsForNewNode(nodeId);
            if (!result.isSuccess()) {
                LOGGER.severe("Failed to rebalance shards for new node " + nodeId + ": " + result.getStatus());
                return Result.failure(result.getStatus());
            }

            LOGGER.info("Node " + nodeId + " added to the cluster successfully");
            return Result.success(null);
        } catch (Exception e) {
            LOGGER.severe("Failed to add node: " + e.getMessage());
            return Result.failure(Status.INTERNAL_ERROR, e.getMessage());
        } finally {
            clusterLock.writeLock().unlock();
        }
    }

    /**
     * Removes a node from the cluster.
     *
     * @param nodeId The node ID
     * @return A result indicating success or failure
     */
    public Result<Void> removeNode(String nodeId) {
        clusterLock.writeLock().lock();
        try {
            LOGGER.info("Removing node " + nodeId + " from the cluster");

            // Check if the node exists
            if (!nodeClients.containsKey(nodeId)) {
                return Result.failure(Status.NOT_FOUND, "Node not found in the cluster");
            }

            // Rebalance shards to exclude the node
            Result<Void> result = rebalanceShardsForRemovedNode(nodeId);
            if (!result.isSuccess()) {
                LOGGER.severe("Failed to rebalance shards for removed node " + nodeId + ": " + result.getStatus());
                return Result.failure(result.getStatus());
            }

            // Remove the node from the consistent hash ring
            consistentHash.removeNode(nodeId);

            // Close the RPC client for the node
            RpcClient client = nodeClients.remove(nodeId);
            result = client.stop();
            if (!result.isSuccess()) {
                LOGGER.warning("Failed to stop RPC client for node " + nodeId + ": " + result.getStatus());
            }

            LOGGER.info("Node " + nodeId + " removed from the cluster successfully");
            return Result.success(null);
        } catch (Exception e) {
            LOGGER.severe("Failed to remove node: " + e.getMessage());
            return Result.failure(Status.INTERNAL_ERROR, e.getMessage());
        } finally {
            clusterLock.writeLock().unlock();
        }
    }

    /**
     * Gets the status of all nodes in the cluster.
     *
     * @return A map of node IDs to their status
     */
    public Map<String, Status> getNodesStatus() {
        clusterLock.readLock().lock();
        try {
            Map<String, Status> statuses = new HashMap<>();
            for (String nodeId : nodeClients.keySet()) {
                Status status = nodeManager.getNodeStatus(nodeId);
                statuses.put(nodeId, status);
            }
            return statuses;
        } finally {
            clusterLock.readLock().unlock();
        }
    }

    /**
     * Gets the status of all shards in the cluster.
     *
     * @return A map of shard IDs to their status
     */
    public Map<String, Status> getShardsStatus() {
        clusterLock.readLock().lock();
        try {
            Map<String, Status> statuses = new HashMap<>();
            for (String shardId : shardToRaftGroup.keySet()) {
                Status status = shardManager.getShardStatus(shardId);
                statuses.put(shardId, status);
            }
            return statuses;
        } finally {
            clusterLock.readLock().unlock();
        }
    }

    /**
     * Gets the nodes that should host a shard based on consistent hashing.
     *
     * @param shardId The shard ID
     * @param count The number of nodes
     * @return A list of node IDs
     */
    private List<String> getNodesForShard(String shardId, int count) {
        List<String> nodes = new ArrayList<>();
        List<String> allNodes = new ArrayList<>(nodeClients.keySet());

        // Simplistic implementation for demo purposes; in a real system,
        // we would use a more sophisticated algorithm that considers node capacity,
        // network topology, etc.
        for (int i = 0; i < Math.min(count, allNodes.size()); i++) {
            nodes.add(allNodes.get(i % allNodes.size()));
        }

        return nodes;
    }

    /**
     * Performs a health check on all nodes in the cluster.
     */
    private void performHealthCheck() {
        try {
            LOGGER.fine("Performing health check on all nodes");
            Map<String, Status> statuses = getNodesStatus();

            for (Map.Entry<String, Status> entry : statuses.entrySet()) {
                String nodeId = entry.getKey();
                Status status = entry.getValue();

                if (status != Status.OK) {
                    LOGGER.warning("Node " + nodeId + " is unhealthy: " + status);

                    // If the node is unreachable, initiate failover
                    if (status == Status.UNAVAILABLE) {
                        initiateFailover(nodeId);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.severe("Error during health check: " + e.getMessage());
        }
    }

    /**
     * Initiates failover for a failed node.
     *
     * @param nodeId The node ID
     */
    private void initiateFailover(String nodeId) {
        try {
            LOGGER.info("Initiating failover for node " + nodeId);

            clusterLock.writeLock().lock();
            try {
                // Find all shards that the node is part of
                for (Map.Entry<String, RaftGroup> entry : shardToRaftGroup.entrySet()) {
                    String shardId = entry.getKey();
                    RaftGroup raftGroup = entry.getValue();

                    if (raftGroup.getMembers().contains(nodeId)) {
                        LOGGER.info("Node " + nodeId + " is part of shard " + shardId + ", initiating failover");

                        // Remove the node from the Raft group
                        Result<Void> result = raftGroup.removeMember(nodeId);
                        if (!result.isSuccess()) {
                            LOGGER.severe("Failed to remove node " + nodeId + " from Raft group for shard " + shardId + ": " + result.getStatus());
                            continue;
                        }

                        // Find a new node to replace the failed one
                        List<String> candidates = new ArrayList<>(nodeClients.keySet());
                        candidates.removeAll(raftGroup.getMembers());

                        if (!candidates.isEmpty()) {
                            String newNodeId = candidates.get(0);
                            LOGGER.info("Adding node " + newNodeId + " to shard " + shardId + " as replacement for failed node " + nodeId);

                            // Add the new node to the Raft group
                            result = raftGroup.addMember(newNodeId);
                            if (!result.isSuccess()) {
                                LOGGER.severe("Failed to add node " + newNodeId + " to Raft group for shard " + shardId + ": " + result.getStatus());
                            }
                        } else {
                            LOGGER.warning("No available nodes to replace failed node " + nodeId + " in shard " + shardId);
                        }
                    }
                }
            } finally {
                clusterLock.writeLock().unlock();
            }
        } catch (Exception e) {
            LOGGER.severe("Error during failover: " + e.getMessage());
        }
    }

    /**
     * Rebalances shards across the cluster.
     */
    private void rebalanceShards() {
        try {
            LOGGER.info("Rebalancing shards across the cluster");

            // This is a simplistic implementation for demo purposes;
            // in a real system, we would have a more sophisticated rebalancing algorithm
            // that considers node capacity, current load, etc.

            // For each shard, check if the current nodes are the optimal ones
            // based on consistent hashing
            for (Map.Entry<String, RaftGroup> entry : shardToRaftGroup.entrySet()) {
                String shardId = entry.getKey();
                RaftGroup raftGroup = entry.getValue();

                List<String> currentNodes = raftGroup.getMembers();
                List<String> optimalNodes = getNodesForShard(shardId, clusterConfig.getReplicationFactor());

                // If the current nodes are different from the optimal ones,
                // initiate data migration
                if (!currentNodes.equals(optimalNodes)) {
                    LOGGER.info("Shard " + shardId + " needs rebalancing");

                    // Find nodes to add and remove
                    List<String> nodesToAdd = new ArrayList<>(optimalNodes);
                    nodesToAdd.removeAll(currentNodes);

                    List<String> nodesToRemove = new ArrayList<>(currentNodes);
                    nodesToRemove.removeAll(optimalNodes);

                    // First add new nodes
                    for (String nodeId : nodesToAdd) {
                        LOGGER.info("Adding node " + nodeId + " to shard " + shardId);

                        Result<Void> result = raftGroup.addMember(nodeId);
                        if (!result.isSuccess()) {
                            LOGGER.severe("Failed to add node " + nodeId + " to Raft group for shard " + shardId + ": " + result.getStatus());
                            continue;
                        }

                        // Perform data migration
                        result = dataMigrator.migrateDataToNode(shardId, nodeId);
                        if (!result.isSuccess()) {
                            LOGGER.severe("Failed to migrate data to node " + nodeId + " for shard " + shardId + ": " + result.getStatus());
                        }
                    }

                    // Then remove old nodes
                    for (String nodeId : nodesToRemove) {
                        LOGGER.info("Removing node " + nodeId + " from shard " + shardId);

                        // Perform data migration
                        Result<Void> result = dataMigrator.migrateDataFromNode(shardId, nodeId);
                        if (!result.isSuccess()) {
                            LOGGER.severe("Failed to migrate data from node " + nodeId + " for shard " + shardId + ": " + result.getStatus());
                            continue;
                        }

                        // Remove the node from the Raft group
                        result = raftGroup.removeMember(nodeId);
                        if (!result.isSuccess()) {
                            LOGGER.severe("Failed to remove node " + nodeId + " from Raft group for shard " + shardId + ": " + result.getStatus());
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.severe("Error during shard rebalancing: " + e.getMessage());
        }
    }

    /**
     * Rebalances shards to include a new node.
     *
     * @param nodeId The node ID
     * @return A result indicating success or failure
     */
    private Result<Void> rebalanceShardsForNewNode(String nodeId) {
        try {
            LOGGER.info("Rebalancing shards to include new node " + nodeId);

            // This is a simplistic implementation for demo purposes;
            // in a real system, we would have a more sophisticated rebalancing algorithm

            // Add the new node to some shards to balance the load
            int shardsToAdd = Math.min(
                clusterConfig.getShardCount() / nodeClients.size(),
                clusterConfig.getMaxShardsPerNode()
            );

            List<String> shardIds = new ArrayList<>(shardToRaftGroup.keySet());
            for (int i = 0; i < Math.min(shardsToAdd, shardIds.size()); i++) {
                String shardId = shardIds.get(i);
                RaftGroup raftGroup = shardToRaftGroup.get(shardId);

                LOGGER.info("Adding node " + nodeId + " to shard " + shardId);

                Result<Void> result = raftGroup.addMember(nodeId);
                if (!result.isSuccess()) {
                    LOGGER.severe("Failed to add node " + nodeId + " to Raft group for shard " + shardId + ": " + result.getStatus());
                    continue;
                }

                // Perform data migration
                result = dataMigrator.migrateDataToNode(shardId, nodeId);
                if (!result.isSuccess()) {
                    LOGGER.severe("Failed to migrate data to node " + nodeId + " for shard " + shardId + ": " + result.getStatus());
                }
            }

            return Result.success(null);
        } catch (Exception e) {
            LOGGER.severe("Error during shard rebalancing for new node: " + e.getMessage());
            return Result.failure(Status.INTERNAL_ERROR, e.getMessage());
        }
    }

    /**
     * Rebalances shards to exclude a removed node.
     *
     * @param nodeId The node ID
     * @return A result indicating success or failure
     */
    private Result<Void> rebalanceShardsForRemovedNode(String nodeId) {
        try {
            LOGGER.info("Rebalancing shards to exclude removed node " + nodeId);

            // Find all shards that the node is part of
            for (Map.Entry<String, RaftGroup> entry : shardToRaftGroup.entrySet()) {
                String shardId = entry.getKey();
                RaftGroup raftGroup = entry.getValue();

                if (raftGroup.getMembers().contains(nodeId)) {
                    LOGGER.info("Node " + nodeId + " is part of shard " + shardId + ", initiating data migration");

                    // Perform data migration
                    Result<Void> result = dataMigrator.migrateDataFromNode(shardId, nodeId);
                    if (!result.isSuccess()) {
                        LOGGER.severe("Failed to migrate data from node " + nodeId + " for shard " + shardId + ": " + result.getStatus());
                        continue;
                    }

                    // Remove the node from the Raft group
                    result = raftGroup.removeMember(nodeId);
                    if (!result.isSuccess()) {
                        LOGGER.severe("Failed to remove node " + nodeId + " from Raft group for shard " + shardId + ": " + result.getStatus());
                        continue;
                    }

                    // Find a new node to replace the removed one
                    List<String> candidates = new ArrayList<>(nodeClients.keySet());
                    candidates.remove(nodeId);
                    candidates.removeAll(raftGroup.getMembers());

                    if (!candidates.isEmpty()) {
                        String newNodeId = candidates.get(0);
                        LOGGER.info("Adding node " + newNodeId + " to shard " + shardId + " as replacement for removed node " + nodeId);

                        // Add the new node to the Raft group
                        result = raftGroup.addMember(newNodeId);
                        if (!result.isSuccess()) {
                            LOGGER.severe("Failed to add node " + newNodeId + " to Raft group for shard " + shardId + ": " + result.getStatus());
                        }

                        // Perform data migration
                        result = dataMigrator.migrateDataToNode(shardId, newNodeId);
                        if (!result.isSuccess()) {
                            LOGGER.severe("Failed to migrate data to node " + newNodeId + " for shard " + shardId + ": " + result.getStatus());
                        }
                    } else {
                        LOGGER.warning("No available nodes to replace removed node " + nodeId + " in shard " + shardId);
                    }
                }
            }

            return Result.success(null);
        } catch (Exception e) {
            LOGGER.severe("Error during shard rebalancing for removed node: " + e.getMessage());
            return Result.failure(Status.INTERNAL_ERROR, e.getMessage());
        }
    }

    /**
     * Routes a message to the appropriate node.
     *
     * @param message The message
     * @return A result containing the response
     */
    public Result<Message> routeMessage(Message message) {
        // Extract the key from the message
        String key = extractKeyFromMessage(message);
        if (key == null) {
            return Result.failure(Status.INVALID_ARGUMENT, "Message does not contain a key");
        }

        // Determine the shard for the key
        String shardId = getShardForKey(key);

        // Get the Raft group for the shard
        RaftGroup raftGroup = getRaftGroupForShard(shardId);
        if (raftGroup == null) {
            return Result.failure(Status.NOT_FOUND, "Shard not found for key: " + key);
        }

        // Route the message to the Raft group leader
        String leaderId = raftGroup.getLeaderId();
        if (leaderId == null) {
            return Result.failure(Status.SERVICE_UNAVAILABLE, "No leader available for shard: " + shardId);
        }

        // Get the RPC client for the leader
        RpcClient client = nodeClients.get(leaderId);
        if (client == null) {
            return Result.failure(Status.NOT_FOUND, "Leader node not found: " + leaderId);
        }

        // Send the message to the leader
        return client.sendMessage(message);
    }

    /**
     * Extracts the key from a message.
     *
     * @param message The message
     * @return The key
     */
    private String extractKeyFromMessage(Message message) {
        // Implementation depends on the message format
        // This is a placeholder
        return message.toString();
    }
}