package com.distributedkv.raft;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distributedkv.common.ConsistentHash;
import com.distributedkv.common.Metrics;
import com.distributedkv.common.MetricsRegistry;
import com.distributedkv.common.Result;
import com.distributedkv.common.Status;
import com.distributedkv.config.ClusterConfig;
import com.distributedkv.config.NodeConfig;
import com.distributedkv.config.RaftConfig;
import com.distributedkv.config.StorageConfig;
import com.distributedkv.network.client.RpcClient;
import com.distributedkv.network.server.RpcServer;
import com.distributedkv.storage.StorageEngine;
import com.distributedkv.storage.StorageManager;
import com.distributedkv.storage.mvcc.MVCCStorage;
import com.distributedkv.storage.sharding.Shard;
import com.distributedkv.storage.sharding.ShardManager;

/**
 * Manages multiple Raft groups for distributed consensus.
 *
 * RaftGroupManager is responsible for creating, starting, stopping, and managing
 * multiple Raft groups. It implements the Multi-Raft architecture mentioned in
 * the requirements, allowing for horizontal scaling and increased throughput.
 */
public class RaftGroupManager {
    private static final Logger LOGGER = Logger.getLogger(RaftGroupManager.class.getName());

    /** The configuration for this node */
    private final NodeConfig nodeConfig;

    /** The configuration for the cluster */
    private final ClusterConfig clusterConfig;

    /** The Raft-specific configuration */
    private final RaftConfig raftConfig;

    /** The storage configuration */
    private final StorageConfig storageConfig;

    /** The RPC client for communicating with other nodes */
    private final RpcClient rpcClient;

    /** The RPC server for receiving requests */
    private final RpcServer rpcServer;

    /** The storage manager */
    private final StorageManager storageManager;

    /** The shard manager */
    private final ShardManager shardManager;

    /** Map of Raft groups by group ID */
    private final Map<String, RaftGroup> groups = new ConcurrentHashMap<>();

    /** Flag indicating whether the manager is running */
    private final AtomicBoolean running = new AtomicBoolean(false);

    /** Scheduler for periodic tasks */
    private final ScheduledExecutorService scheduler;

    /** Consistent hash for key distribution */
    private final ConsistentHash<String> consistentHash;

    /** Metrics for monitoring manager performance */
    private final Metrics metrics;

    /**
     * Creates a new Raft group manager.
     *
     * @param nodeConfig The node configuration
     * @param clusterConfig The cluster configuration
     * @param raftConfig The Raft configuration
     * @param storageConfig The storage configuration
     * @param rpcClient The RPC client
     * @param rpcServer The RPC server
     * @param storageManager The storage manager
     * @param shardManager The shard manager
     */
    public RaftGroupManager(NodeConfig nodeConfig, ClusterConfig clusterConfig,
                           RaftConfig raftConfig, StorageConfig storageConfig,
                           RpcClient rpcClient, RpcServer rpcServer,
                           StorageManager storageManager, ShardManager shardManager) {
        this.nodeConfig = nodeConfig;
        this.clusterConfig = clusterConfig;
        this.raftConfig = raftConfig;
        this.storageConfig = storageConfig;
        this.rpcClient = rpcClient;
        this.rpcServer = rpcServer;
        this.storageManager = storageManager;
        this.shardManager = shardManager;
        this.scheduler = new ScheduledThreadPoolExecutor(2, r -> {
            Thread t = new Thread(r, "raft-group-manager-scheduler");
            t.setDaemon(true);
            return t;
        });
        this.consistentHash = new ConsistentHash<>(clusterConfig.getNumVirtualNodes());
        this.metrics = MetricsRegistry.getInstance().getOrCreateMetrics("RaftGroupManager");
    }

    /**
     * Initializes the manager.
     *
     * @return A result indicating success or failure
     */
    public Result<Void> initialize() {
        LOGGER.info("Initializing Raft group manager");

        try {
            // Initialize shard manager
            shardManager.initialize();

            // Get all shards assigned to this node
            Set<Shard> nodeShards = shardManager.getNodeShards(nodeConfig.getNodeId());

            // Initialize groups for all shards
            for (Shard shard : nodeShards) {
                initializeGroupForShard(shard);
            }

            // Initialize consistent hash
            for (String groupId : groups.keySet()) {
                consistentHash.add(groupId);
            }

            return Result.success(null);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to initialize Raft group manager", e);
            return Result.failure("Failed to initialize Raft group manager: " + e.getMessage());
        }
    }

    /**
     * Initializes a Raft group for a shard.
     *
     * @param shard The shard
     * @throws IOException If an I/O error occurs
     */
    private void initializeGroupForShard(Shard shard) throws IOException {
        String groupId = shard.getShardId();

        LOGGER.info("Initializing Raft group for shard " + groupId);

        // Create storage engine for this group
        StorageEngine storageEngine = storageManager.createStorageEngine(
                storageConfig.getEngineType(), groupId);

        // Create MVCC storage if enabled
        MVCCStorage mvccStorage = null;
        if (storageConfig.isMvccEnabled()) {
            mvccStorage = storageManager.createMVCCStorage(storageEngine);
        }

        // Create state machine
        KVStateMachine stateMachine = new KVStateMachine(
                storageEngine, storageConfig.isMvccEnabled(), mvccStorage);

        // Create Raft storage
        RaftStorage raftStorage = storageManager.createRaftStorage(groupId);

        // Create log
        Log log = new Log(raftStorage);

        // Get peer configs for this group
        Map<String, String> peerEndpoints = new HashMap<>();
        for (String peerId : shard.getReplicaNodeIds()) {
            if (!peerId.equals(nodeConfig.getNodeId())) {
                String endpoint = clusterConfig.getNodeEndpoint(peerId);
                if (endpoint != null) {
                    peerEndpoints.put(peerId, endpoint);
                }
            }
        }

        // Create peers
        Map<String, RaftPeer> peers = new HashMap<>();
        for (Map.Entry<String, String> entry : peerEndpoints.entrySet()) {
            String peerId = entry.getKey();
            String endpoint = entry.getValue();
            peers.put(peerId, new RaftPeer(peerId, endpoint, 0, rpcClient, groupId));
        }

        // Create Raft node
        RaftNode raftNode = new RaftNode(
                nodeConfig.getNodeId(), groupId, nodeConfig, raftConfig,
                log, stateMachine, raftStorage, rpcClient, peers);

        // Create Raft group
        RaftGroup raftGroup = new RaftGroup(groupId, raftNode, stateMachine, raftConfig);

        // Initialize group
        Result<Void> result = raftGroup.initialize();
        if (result.isFailure()) {
            throw new IOException("Failed to initialize Raft group: " + result.getError());
        }

        // Store group
        groups.put(groupId, raftGroup);

        LOGGER.info("Initialized Raft group for shard " + groupId);
    }

    /**
     * Starts the manager.
     *
     * @return A result indicating success or failure
     */
    public Result<Void> start() {
        if (running.compareAndSet(false, true)) {
            LOGGER.info("Starting Raft group manager");

            try {
                // Start all groups
                for (RaftGroup group : groups.values()) {
                    Result<Void> result = group.start();
                    if (result.isFailure()) {
                        LOGGER.log(Level.WARNING, "Failed to start Raft group " + group.getGroupId() +
                                ": " + result.getError());
                    }
                }

                // Schedule health check
                scheduler.scheduleAtFixedRate(
                        this::checkGroupHealth,
                        raftConfig.getHealthCheckIntervalMs(),
                        raftConfig.getHealthCheckIntervalMs(),
                        TimeUnit.MILLISECONDS);

                return Result.success(null);
            } catch (Exception e) {
                running.set(false);
                LOGGER.log(Level.SEVERE, "Failed to start Raft group manager", e);
                return Result.failure("Failed to start Raft group manager: " + e.getMessage());
            }
        }

        return Result.success(null);
    }

    /**
     * Stops the manager.
     *
     * @return A result indicating success or failure
     */
    public Result<Void> stop() {
        if (running.compareAndSet(true, false)) {
            LOGGER.info("Stopping Raft group manager");

            try {
                // Shutdown scheduler
                scheduler.shutdown();
                try {
                    scheduler.awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOGGER.warning("Interrupted while waiting for scheduler to shutdown");
                }

                // Stop all groups
                for (RaftGroup group : groups.values()) {
                    Result<Void> result = group.stop();
                    if (result.isFailure()) {
                        LOGGER.log(Level.WARNING, "Failed to stop Raft group " + group.getGroupId() +
                                ": " + result.getError());
                    }
                }

                return Result.success(null);
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Failed to stop Raft group manager", e);
                return Result.failure("Failed to stop Raft group manager: " + e.getMessage());
            }
        }

        return Result.success(null);
    }

    /**
     * Checks the health of all groups.
     */
    private void checkGroupHealth() {
        if (!running.get()) {
            return;
        }

        LOGGER.fine("Checking health of all Raft groups");

        for (RaftGroup group : groups.values()) {
            try {
                Status status = group.getStatus();

                // Log if not running or no leader
                if (!status.isRunning()) {
                    LOGGER.warning("Raft group " + group.getGroupId() + " is not running");
                } else if (status.getLeaderId() == null) {
                    LOGGER.warning("Raft group " + group.getGroupId() + " has no leader");
                }

                // TODO: Add more health checks

            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Failed to check health of Raft group " +
                        group.getGroupId(), e);
            }
        }
    }

    /**
     * Gets the Raft group for a key.
     *
     * @param key The key
     * @return The Raft group, or null if none found
     */
    public RaftGroup getGroupForKey(byte[] key) {
        if (groups.isEmpty()) {
            return null;
        }

        // Use consistent hashing to determine the group for this key
        String groupId = consistentHash.get(new String(key));

        return groups.get(groupId);
    }

    /**
     * Gets a Raft group by ID.
     *
     * @param groupId The group ID
     * @return The Raft group, or null if not found
     */
    public RaftGroup getGroup(String groupId) {
        return groups.get(groupId);
    }

    /**
     * Gets all Raft groups.
     *
     * @return The Raft groups
     */
    public Collection<RaftGroup> getAllGroups() {
        return Collections.unmodifiableCollection(groups.values());
    }

    /**
     * Gets the number of Raft groups.
     *
     * @return The number of groups
     */
    public int getGroupCount() {
        return groups.size();
    }

    /**
     * Puts a key-value pair.
     *
     * @param key The key
     * @param value The value
     * @return A future that completes with a result indicating success or failure
     */
    public CompletableFuture<Result<Void>> put(byte[] key, byte[] value) {
        if (!running.get()) {
            return CompletableFuture.completedFuture(
                    Result.failure("Manager is not running"));
        }

        metrics.counter("put").inc();

        // Get the group for this key
        RaftGroup group = getGroupForKey(key);
        if (group == null) {
            metrics.counter("put.no_group").inc();
            return CompletableFuture.completedFuture(
                    Result.failure("No group found for key"));
        }

        // Forward to the group
        return group.put(key, value)
                .thenApply(result -> {
                    if (result.isSuccess()) {
                        metrics.counter("put.success").inc();
                    } else {
                        metrics.counter("put.failure").inc();
                    }
                    return result;
                })
                .exceptionally(ex -> {
                    metrics.counter("put.error").inc();
                    LOGGER.log(Level.WARNING, "Failed to put key", ex);
                    return Result.failure("Failed to put key: " + ex.getMessage());
                });
    }

    /**
     * Gets the value for a key.
     *
     * @param key The key
     * @return A future that completes with a result containing the value or an error
     */
    public CompletableFuture<Result<byte[]>> get(byte[] key) {
        if (!running.get()) {
            return CompletableFuture.completedFuture(
                    Result.failure("Manager is not running"));
        }

        metrics.counter("get").inc();

        // Get the group for this key
        RaftGroup group = getGroupForKey(key);
        if (group == null) {
            metrics.counter("get.no_group").inc();
            return CompletableFuture.completedFuture(
                    Result.failure("No group found for key"));
        }

        // Forward to the group
        return group.get(key)
                .thenApply(result -> {
                    if (result.isSuccess()) {
                        metrics.counter("get.success").inc();
                    } else {
                        metrics.counter("get.failure").inc();
                    }
                    return result;
                })
                .exceptionally(ex -> {
                    metrics.counter("get.error").inc();
                    LOGGER.log(Level.WARNING, "Failed to get key", ex);
                    return Result.failure("Failed to get key: " + ex.getMessage());
                });
    }

    /**
     * Gets the value for a key using follower read.
     *
     * @param key The key
     * @return A future that completes with a result containing the value or an error
     */
    public CompletableFuture<Result<byte[]>> getWithFollowerRead(byte[] key) {
        if (!running.get()) {
            return CompletableFuture.completedFuture(
                    Result.failure("Manager is not running"));
        }

        metrics.counter("get.followerread").inc();

        // Get the group for this key
        RaftGroup group = getGroupForKey(key);
        if (group == null) {
            metrics.counter("get.followerread.no_group").inc();
            return CompletableFuture.completedFuture(
                    Result.failure("No group found for key"));
        }

        // Forward to the group
        return group.getWithFollowerRead(key)
                .thenApply(result -> {
                    if (result.isSuccess()) {
                        metrics.counter("get.followerread.success").inc();
                    } else {
                        metrics.counter("get.followerread.failure").inc();
                    }
                    return result;
                })
                .exceptionally(ex -> {
                    metrics.counter("get.followerread.error").inc();
                    LOGGER.log(Level.WARNING, "Failed to get key with follower read", ex);
                    return Result.failure("Failed to get key with follower read: " + ex.getMessage());
                });
    }

    /**
     * Deletes a key.
     *
     * @param key The key
     * @return A future that completes with a result indicating success or failure
     */
    public CompletableFuture<Result<Boolean>> delete(byte[] key) {
        if (!running.get()) {
            return CompletableFuture.completedFuture(
                    Result.failure("Manager is not running"));
        }

        metrics.counter("delete").inc();

        // Get the group for this key
        RaftGroup group = getGroupForKey(key);
        if (group == null) {
            metrics.counter("delete.no_group").inc();
            return CompletableFuture.completedFuture(
                    Result.failure("No group found for key"));
        }

        // Forward to the group
        return group.delete(key)
                .thenApply(result -> {
                    if (result.isSuccess()) {
                        metrics.counter("delete.success").inc();
                    } else {
                        metrics.counter("delete.failure").inc();
                    }
                    return result;
                })
                .exceptionally(ex -> {
                    metrics.counter("delete.error").inc();
                    LOGGER.log(Level.WARNING, "Failed to delete key", ex);
                    return Result.failure("Failed to delete key: " + ex.getMessage());
                });
    }

    /**
     * Creates a client session.
     *
     * @return The session ID
     */
    public String createClientSession() {
        // For now, just return a random session ID
        return java.util.UUID.randomUUID().toString();
    }

    /**
     * Closes a client session.
     *
     * @param sessionId The session ID
     */
    public void closeClientSession(String sessionId) {
        // Nothing to do for now
    }

    /**
     * Gets the status of all groups.
     *
     * @return Map of group ID to status
     */
    public Map<String, Status> getAllGroupStatus() {
        Map<String, Status> statusMap = new HashMap<>();

        for (RaftGroup group : groups.values()) {
            statusMap.put(group.getGroupId(), group.getStatus());
        }

        return statusMap;
    }
}