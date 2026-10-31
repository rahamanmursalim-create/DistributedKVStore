package com.distributedkv.server;

import com.distributedkv.common.Result;
import com.distributedkv.common.Status;
import com.distributedkv.common.Metrics;
import com.distributedkv.common.MetricsRegistry;
import com.distributedkv.config.NodeConfig;
import com.distributedkv.config.RaftConfig;
import com.distributedkv.config.StorageConfig;
import com.distributedkv.network.client.RpcClient;
import com.distributedkv.network.server.RpcServer;
import com.distributedkv.network.protocol.Message;
import com.distributedkv.network.protocol.Request;
import com.distributedkv.network.protocol.Response;
import com.distributedkv.raft.RaftNode;
import com.distributedkv.raft.StateMachine;
import com.distributedkv.raft.KVStateMachine;
import com.distributedkv.storage.StorageEngine;
import com.distributedkv.storage.StorageManager;
import com.distributedkv.storage.mvcc.MVCCStorage;
import com.distributedkv.storage.mvcc.TransactionManager;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
import java.util.Collections;

/**
 * NodeManager is responsible for managing the lifecycle and operations of a single node
 * in the distributed key-value store. It coordinates the storage engines, Raft nodes,
 * and network components to ensure proper functioning of the node.
 */
public class NodeManager {
    private static final Logger LOGGER = Logger.getLogger(NodeManager.class.getName());

    // Configuration for the node
    private final NodeConfig nodeConfig;
    private final RaftConfig raftConfig;
    private final StorageConfig storageConfig;

    // RPC server for handling incoming requests
    private final RpcServer rpcServer;

    // Storage manager for managing storage engines
    private final StorageManager storageManager;

    // Map of Raft node ID to Raft node instances
    private final Map<String, RaftNode> raftNodes;

    // Map of storage engine ID to storage engine instances
    private final Map<String, StorageEngine> storageEngines;

    // Map of state machine ID to state machine instances
    private final Map<String, StateMachine> stateMachines;

    // Transaction manager for handling MVCC transactions
    private final TransactionManager transactionManager;

    // Metrics registry for collecting and reporting metrics
    private final MetricsRegistry metricsRegistry;

    // Status of the node
    private final AtomicReference<Status> nodeStatus;

    // Flag indicating whether the node is running
    private final AtomicBoolean isRunning;

    // Scheduled executor for background tasks
    private final ScheduledExecutorService scheduler;

    /**
     * Creates a new node manager with the given configurations.
     *
     * @param nodeConfig The node configuration
     * @param raftConfig The Raft configuration
     * @param storageConfig The storage configuration
     * @param rpcServer The RPC server
     * @param storageManager The storage manager
     * @param transactionManager The transaction manager
     */
    public NodeManager(NodeConfig nodeConfig, RaftConfig raftConfig, StorageConfig storageConfig,
                      RpcServer rpcServer, StorageManager storageManager,
                      TransactionManager transactionManager) {
        this.nodeConfig = nodeConfig;
        this.raftConfig = raftConfig;
        this.storageConfig = storageConfig;
        this.rpcServer = rpcServer;
        this.storageManager = storageManager;
        this.transactionManager = transactionManager;

        this.raftNodes = new ConcurrentHashMap<>();
        this.storageEngines = new ConcurrentHashMap<>();
        this.stateMachines = new ConcurrentHashMap<>();
        this.metricsRegistry = new MetricsRegistry();
        this.nodeStatus = new AtomicReference<>(Status.UNAVAILABLE);
        this.isRunning = new AtomicBoolean(false);
        this.scheduler = new ScheduledThreadPoolExecutor(
            nodeConfig.getSchedulerThreads(),
            r -> {
                Thread thread = new Thread(r, "node-scheduler");
                thread.setDaemon(true);
                return thread;
            }
        );
    }

    /**
     * Starts the node manager and all its components.
     *
     * @return A result indicating success or failure
     */
    public Result<Void> start() {
        if (isRunning.getAndSet(true)) {
            return Result.success(null); // Already running
        }

        LOGGER.info("Starting node manager for node " + nodeConfig.getNodeId());

        try {
            // Initialize and start storage engines
            Result<Void> result = initializeStorageEngines();
            if (!result.isSuccess()) {
                LOGGER.severe("Failed to initialize storage engines: " + result.getStatus());
                return result;
            }

            // Initialize and start state machines
            result = initializeStateMachines();
            if (!result.isSuccess()) {
                LOGGER.severe("Failed to initialize state machines: " + result.getStatus());
                return result;
            }

            // Initialize and start Raft nodes
            result = initializeRaftNodes();
            if (!result.isSuccess()) {
                LOGGER.severe("Failed to initialize Raft nodes: " + result.getStatus());
                return result;
            }

            // Start the RPC server
            result = rpcServer.start();
            if (!result.isSuccess()) {
                LOGGER.severe("Failed to start RPC server: " + result.getStatus());
                return result;
            }

            // Register handlers for network requests
            registerRequestHandlers();

            // Start background tasks
            startBackgroundTasks();

            // Set node status to OK
            nodeStatus.set(Status.OK);

            LOGGER.info("Node manager started successfully for node " + nodeConfig.getNodeId());
            return Result.success(null);
        } catch (Exception e) {
            LOGGER.severe("Failed to start node manager: " + e.getMessage());
            stop(); // Cleanup
            return Result.failure(Status.INTERNAL_ERROR, e.getMessage());
        }
    }

    /**
     * Stops the node manager and all its components.
     *
     * @return A result indicating success or failure
     */
    public Result<Void> stop() {
        if (!isRunning.getAndSet(false)) {
            return Result.success(null); // Already stopped
        }

        LOGGER.info("Stopping node manager for node " + nodeConfig.getNodeId());

        try {
            // Stop the scheduler
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }

            // Stop the RPC server
            Result<Void> result = rpcServer.stop();
            if (!result.isSuccess()) {
                LOGGER.warning("Failed to stop RPC server: " + result.getStatus());
            }

            // Stop all Raft nodes
            for (Map.Entry<String, RaftNode> entry : raftNodes.entrySet()) {
                String raftId = entry.getKey();
                RaftNode raftNode = entry.getValue();

                result = raftNode.stop();
                if (!result.isSuccess()) {
                    LOGGER.warning("Failed to stop Raft node " + raftId + ": " + result.getStatus());
                }
            }

            // Close all state machines
            for (Map.Entry<String, StateMachine> entry : stateMachines.entrySet()) {
                String smId = entry.getKey();
                StateMachine stateMachine = entry.getValue();

                result = stateMachine.close();
                if (!result.isSuccess()) {
                    LOGGER.warning("Failed to close state machine " + smId + ": " + result.getStatus());
                }
            }

            // Close all storage engines
            for (Map.Entry<String, StorageEngine> entry : storageEngines.entrySet()) {
                String engineId = entry.getKey();
                StorageEngine engine = entry.getValue();

                result = engine.close();
                if (!result.isSuccess()) {
                    LOGGER.warning("Failed to close storage engine " + engineId + ": " + result.getStatus());
                }
            }

            // Clear collections
            raftNodes.clear();
            stateMachines.clear();
            storageEngines.clear();

            // Set node status to UNAVAILABLE
            nodeStatus.set(Status.UNAVAILABLE);

            LOGGER.info("Node manager stopped successfully for node " + nodeConfig.getNodeId());
            return Result.success(null);
        } catch (Exception e) {
            LOGGER.severe("Failed to stop node manager: " + e.getMessage());
            return Result.failure(Status.INTERNAL_ERROR, e.getMessage());
        }
    }

    /**
     * Gets the status of the node.
     *
     * @return The current status of the node
     */
    public Status getNodeStatus() {
        return nodeStatus.get();
    }

    /**
     * Gets the status of a specific node given its ID.
     * This is used by the ClusterManager to check the status of other nodes.
     *
     * @param nodeId The ID of the node to check
     * @return The status of the node
     */
    public Status getNodeStatus(String nodeId) {
        if (nodeId.equals(nodeConfig.getNodeId())) {
            return getNodeStatus();
        } else {
            // For remote nodes, we need to query them via RPC
            // This is a simplified implementation; in a real system,
            // we would use a more sophisticated approach with caching and timeouts
            RpcClient client = new RpcClient(nodeConfig.getHost(), nodeConfig.getPort());
            try {
                Result<Void> result = client.start();
                if (!result.isSuccess()) {
                    return Status.UNAVAILABLE;
                }

                Request request = new Request();
                request.setType(Request.Type.STATUS);

                Result<Response> responseResult = client.sendRequest(request);
                client.stop();

                if (!responseResult.isSuccess()) {
                    return Status.UNAVAILABLE;
                }

                Response response = responseResult.getValue();
                return response.getStatus();
            } catch (Exception e) {
                LOGGER.warning("Failed to get status of node " + nodeId + ": " + e.getMessage());
                return Status.UNAVAILABLE;
            }
        }
    }

    /**
     * Gets the metrics for this node.
     *
     * @return The metrics for this node
     */
    public Metrics getMetrics() {
        return metricsRegistry.getMetrics();
    }

    /**
     * Creates and initializes a new Raft node.
     *
     * @param raftId The ID of the Raft node
     * @param stateMachine The state machine for the Raft node
     * @param storage The storage engine for the Raft node
     * @return A result indicating success or failure
     */
    public Result<RaftNode> createRaftNode(String raftId, StateMachine stateMachine, StorageEngine storage) {
        if (raftNodes.containsKey(raftId)) {
            return Result.failure(Status.ALREADY_EXISTS, "Raft node already exists: " + raftId);
        }

        try {
            RaftNode raftNode = new RaftNode(
                raftId,
                nodeConfig.getNodeId(),
                raftConfig,
                stateMachine,
                storage,
                metricsRegistry
            );

            Result<Void> result = raftNode.initialize();
            if (!result.isSuccess()) {
                return Result.failure(result.getStatus(), "Failed to initialize Raft node: " + raftId);
            }

            raftNodes.put(raftId, raftNode);
            return Result.success(raftNode);
        } catch (Exception e) {
            LOGGER.severe("Failed to create Raft node " + raftId + ": " + e.getMessage());
            return Result.failure(Status.INTERNAL_ERROR, e.getMessage());
        }
    }

    /**
     * Gets a Raft node by its ID.
     *
     * @param raftId The ID of the Raft node
     * @return The Raft node, or null if not found
     */
    public RaftNode getRaftNode(String raftId) {
        return raftNodes.get(raftId);
    }

    /**
     * Gets all Raft nodes managed by this node manager.
     *
     * @return An unmodifiable map of Raft node ID to Raft node instances
     */
    public Map<String, RaftNode> getRaftNodes() {
        return Collections.unmodifiableMap(raftNodes);
    }

    /**
     * Processes a request message.
     *
     * @param request The request message
     * @return A response message
     */
    public Response processRequest(Request request) {
        LOGGER.fine("Processing request: " + request.getType());

        try {
            switch (request.getType()) {
                case STATUS:
                    return handleStatusRequest(request);
                case RAFT:
                    return handleRaftRequest(request);
                case KV:
                    return handleKVRequest(request);
                default:
                    LOGGER.warning("Unknown request type: " + request.getType());
                    return new Response(Status.INVALID_ARGUMENT, "Unknown request type");
            }
        } catch (Exception e) {
            LOGGER.severe("Failed to process request: " + e.getMessage());
            return new Response(Status.INTERNAL_ERROR, e.getMessage());
        }
    }

    /**
     * Handles a status request.
     *
     * @param request The request
     * @return The response
     */
    private Response handleStatusRequest(Request request) {
        Response response = new Response();
        response.setStatus(getNodeStatus());
        return response;
    }

    /**
     * Handles a Raft request.
     *
     * @param request The request
     * @return The response
     */
    private Response handleRaftRequest(Request request) {
        String raftId = request.getRaftGroupId();
        if (raftId == null) {
            return new Response(Status.INVALID_ARGUMENT, "Raft group ID is missing");
        }

        RaftNode raftNode = raftNodes.get(raftId);
        if (raftNode == null) {
            return new Response(Status.NOT_FOUND, "Raft node not found: " + raftId);
        }

        return raftNode.handleRequest(request);
    }

    /**
     * Handles a KV request.
     *
     * @param request The request
     * @return The response
     */
    private Response handleKVRequest(Request request) {
        String key = request.getKey();
        if (key == null) {
            return new Response(Status.INVALID_ARGUMENT, "Key is missing");
        }

        // Determine which Raft group the key belongs to
        // This is a simplified implementation; in a real system,
        // we would use consistent hashing or another partitioning scheme
        String raftId = "default";

        RaftNode raftNode = raftNodes.get(raftId);
        if (raftNode == null) {
            return new Response(Status.NOT_FOUND, "Raft node not found: " + raftId);
        }

        // Process the request through the Raft node
        return raftNode.handleRequest(request);
    }

    /**
     * Initializes the storage engines.
     *
     * @return A result indicating success or failure
     */
}
