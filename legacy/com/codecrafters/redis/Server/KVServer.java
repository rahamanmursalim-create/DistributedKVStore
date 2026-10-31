package com.distributedkv.server;

import com.distributedkv.common.Result;
import com.distributedkv.common.Status;
import com.distributedkv.config.ClusterConfig;
import com.distributedkv.config.NodeConfig;
import com.distributedkv.config.RaftConfig;
import com.distributedkv.config.StorageConfig;
import com.distributedkv.network.protocol.KVMessage;
import com.distributedkv.network.protocol.Request;
import com.distributedkv.network.protocol.Response;
import com.distributedkv.network.server.NettyServer;
import com.distributedkv.network.server.RpcServer;
import com.distributedkv.raft.KVStateMachine;
import com.distributedkv.raft.RaftGroup;
import com.distributedkv.raft.RaftGroupManager;
import com.distributedkv.storage.StorageEngine;
import com.distributedkv.storage.StorageManager;
import com.distributedkv.storage.sharding.Shard;
import com.distributedkv.storage.sharding.ShardManager;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * KVServer represents a node in the distributed key-value store cluster.
 * It handles client requests, manages Raft groups, and coordinates with the cluster.
 */
public class KVServer {
    private static final Logger logger = Logger.getLogger(KVServer.class.getName());

    private final String nodeId;
    private final NodeConfig nodeConfig;
    private final ClusterConfig clusterConfig;
    private final RaftConfig raftConfig;
    private final StorageConfig storageConfig;

    private final RpcServer rpcServer;
    private final ClusterManager clusterManager;
    private final NodeManager nodeManager;
    private final RaftGroupManager raftGroupManager;
    private final StorageManager storageManager;
    private final ShardManager shardManager;

    private volatile boolean running;

    public KVServer(String nodeId, NodeConfig nodeConfig, ClusterConfig clusterConfig,
                   RaftConfig raftConfig, StorageConfig storageConfig) {
        this.nodeId = nodeId;
        this.nodeConfig = nodeConfig;
        this.clusterConfig = clusterConfig;
        this.raftConfig = raftConfig;
        this.storageConfig = storageConfig;

        // Initialize managers
        this.storageManager = new StorageManager(storageConfig);
        this.shardManager = new ShardManager(clusterConfig);
        this.raftGroupManager = new RaftGroupManager(nodeId, raftConfig, storageManager);
        this.clusterManager = new ClusterManager(nodeId, clusterConfig, shardManager, raftGroupManager);
        this.nodeManager = new NodeManager(nodeId, nodeConfig, clusterManager);

        // Initialize RPC server
        this.rpcServer = new NettyServer(nodeConfig.getServerHost(), nodeConfig.getServerPort(), this::handleRequest);

        this.running = false;
    }

    /**
     * Starts the KV server.
     */
    public void start() {
        if (running) {
            return;
        }

        logger.info("Starting KV server with ID: " + nodeId);

        // Start components in the correct order
        try {
            // Start storage engines
            storageManager.start();

            // Initialize shards
            shardManager.initialize();

            // Start Raft groups
            raftGroupManager.start();

            // Initialize node and cluster manager
            nodeManager.start();
            clusterManager.start();

            // Start RPC server to accept client requests
            rpcServer.start();

            running = true;
            logger.info("KV server started successfully");
        } catch (Exception e) {
            logger.severe("Failed to start KV server: " + e.getMessage());
            stop();
            throw new RuntimeException("Failed to start KV server", e);
        }
    }

    /**
     * Stops the KV server.
     */
    public void stop() {
        if (!running) {
            return;
        }

        logger.info("Stopping KV server with ID: " + nodeId);

        // Stop components in the reverse order
        try {
            // Stop RPC server
            rpcServer.stop();

            // Stop cluster and node managers
            clusterManager.stop();
            nodeManager.stop();

            // Stop Raft groups
            raftGroupManager.stop();

            // Stop storage engines
            storageManager.stop();

            running = false;
            logger.info("KV server stopped successfully");
        } catch (Exception e) {
            logger.severe("Error while stopping KV server: " + e.getMessage());
            throw new RuntimeException("Failed to stop KV server", e);
        }
    }

    /**
     * Handles incoming client requests.
     *
     * @param request The incoming request
     * @return A CompletableFuture that will be completed with the response
     */
    private CompletableFuture<Response> handleRequest(Request request) {
        CompletableFuture<Response> future = new CompletableFuture<>();

        try {
            if (request.getType() == Request.Type.KV_REQUEST) {
                handleKVRequest(request, future);
            } else if (request.getType() == Request.Type.ADMIN_REQUEST) {
                handleAdminRequest(request, future);
            } else {
                future.complete(new Response(Response.Status.INVALID_REQUEST,
                                "Unsupported request type: " + request.getType()));
            }
        } catch (Exception e) {
            logger.severe("Error handling request: " + e.getMessage());
            future.complete(new Response(Response.Status.ERROR, "Internal server error: " + e.getMessage()));
        }

        return future;
    }

    /**
     * Handles a key-value operation request.
     */
    private void handleKVRequest(Request request, CompletableFuture<Response> future) {
        KVMessage kvMessage = (KVMessage) request.getMessage();
        String key = kvMessage.getKey();

        // Find the responsible shard and Raft group for this key
        Shard shard = shardManager.getShardForKey(key);
        if (shard == null) {
            future.complete(new Response(Response.Status.ERROR, "No shard found for key: " + key));
            return;
        }

        RaftGroup raftGroup = raftGroupManager.getRaftGroup(shard.getRaftGroupId());
        if (raftGroup == null) {
            future.complete(new Response(Response.Status.ERROR,
                            "Raft group not found for shard: " + shard.getShardId()));
            return;
        }

        // Process the request based on its type
        switch (kvMessage.getType()) {
            case GET:
                handleGetRequest(key, raftGroup, future);
                break;
            case PUT:
                handlePutRequest(key, kvMessage.getValue(), raftGroup, future);
                break;
            case DELETE:
                handleDeleteRequest(key, raftGroup, future);
                break;
            default:
                future.complete(new Response(Response.Status.INVALID_REQUEST,
                                "Unsupported KV operation: " + kvMessage.getType()));
        }
    }

    /**
     * Handles a GET request.
     */
    private void handleGetRequest(String key, RaftGroup raftGroup, CompletableFuture<Response> future) {
        // Use ReadIndex for linearizable reads
        raftGroup.readIndex().thenAccept(readStatus -> {
            if (readStatus.isOk()) {
                // Apply read operation to state machine
                KVStateMachine stateMachine = (KVStateMachine) raftGroup.getStateMachine();
                Result<byte[]> result = stateMachine.get(key);

                if (result.getStatus().isOk()) {
                    Response response = new Response(Response.Status.OK, result.getValue());
                    future.complete(response);
                } else {
                    Response response = new Response(Response.Status.ERROR,
                                        "Failed to get value: " + result.getStatus().getMessage());
                    future.complete(response);
                }
            } else {
                future.complete(new Response(Response.Status.ERROR,
                                "ReadIndex failed: " + readStatus.getMessage()));
            }
        }).exceptionally(ex -> {
            future.complete(new Response(Response.Status.ERROR,
                            "Exception during read: " + ex.getMessage()));
            return null;
        });
    }

    /**
     * Handles a PUT request.
     */
    private void handlePutRequest(String key, byte[] value, RaftGroup raftGroup,
                                CompletableFuture<Response> future) {
        // Create a command to be processed by the Raft consensus
        KVMessage.Command command = new KVMessage.Command(KVMessage.CommandType.PUT, key, value);
        byte[] commandBytes = command.serialize();

        // Propose the command to the Raft group
        raftGroup.propose(commandBytes).thenAccept(proposeStatus -> {
            if (proposeStatus.isOk()) {
                future.complete(new Response(Response.Status.OK, "Value successfully set"));
            } else {
                future.complete(new Response(Response.Status.ERROR,
                                "Failed to set value: " + proposeStatus.getMessage()));
            }
        }).exceptionally(ex -> {
            future.complete(new Response(Response.Status.ERROR,
                            "Exception during write: " + ex.getMessage()));
            return null;
        });
    }

    /**
     * Handles a DELETE request.
     */
    private void handleDeleteRequest(String key, RaftGroup raftGroup,
                                   CompletableFuture<Response> future) {
        // Create a command to be processed by the Raft consensus
        KVMessage.Command command = new KVMessage.Command(KVMessage.CommandType.DELETE, key, null);
        byte[] commandBytes = command.serialize();

        // Propose the command to the Raft group
        raftGroup.propose(commandBytes).thenAccept(proposeStatus -> {
            if (proposeStatus.isOk()) {
                future.complete(new Response(Response.Status.OK, "Value successfully deleted"));
            } else {
                future.complete(new Response(Response.Status.ERROR,
                                "Failed to delete value: " + proposeStatus.getMessage()));
            }
        }).exceptionally(ex -> {
            future.complete(new Response(Response.Status.ERROR,
                            "Exception during delete: " + ex.getMessage()));
            return null;
        });
    }

    /**
     * Handles administrative requests like cluster membership changes.
     */
    private void handleAdminRequest(Request request, CompletableFuture<Response> future) {
        // Admin requests are handled by the cluster manager
        clusterManager.handleAdminRequest(request, future);
    }

    /**
     * Gets the node ID of this KV server.
     */
    public String getNodeId() {
        return nodeId;
    }

    /**
     * Checks if the server is currently running.
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * Gets the cluster manager.
     */
    public ClusterManager getClusterManager() {
        return clusterManager;
    }

    /**
     * Gets the node manager.
     */
    public NodeManager getNodeManager() {
        return nodeManager;
    }
}