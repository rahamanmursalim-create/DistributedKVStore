package com.distributedkv.raft;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distributedkv.common.Metrics;
import com.distributedkv.common.MetricsRegistry;
import com.distributedkv.common.Result;
import com.distributedkv.common.Status;
import com.distributedkv.config.RaftConfig;
import com.distributedkv.network.client.RpcClient;
import com.distributedkv.network.server.RpcServer;

/**
 * Manages a group of nodes that participate in the Raft consensus protocol.
 *
 * RaftGroup represents a single consensus group with its own state machine.
 * It manages the lifecycle of the Raft node and provides methods for
 * interacting with the replicated state machine.
 */
public class RaftGroup {
    private static final Logger LOGGER = Logger.getLogger(RaftGroup.class.getName());

    /** The ID of this Raft group */
    private final String groupId;

    /** The Raft node for this group */
    private final RaftNode raftNode;

    /** The state machine for this group */
    private final StateMachine stateMachine;

    /** The Raft-specific configuration */
    private final RaftConfig raftConfig;

    /** Flag indicating whether this group is running */
    private final AtomicBoolean running = new AtomicBoolean(false);

    /** The ID of the current leader node */
    private final AtomicReference<String> leaderId = new AtomicReference<>(null);

    /** Map of client sessions */
    private final Map<String, ClientSession> clientSessions = new ConcurrentHashMap<>();

    /** Metrics for monitoring group performance */
    private final Metrics metrics;

    /**
     * Creates a new Raft group.
     *
     * @param groupId The ID of this group
     * @param raftNode The Raft node for this group
     * @param stateMachine The state machine for this group
     * @param raftConfig The Raft configuration
     */
    public RaftGroup(String groupId, RaftNode raftNode, StateMachine stateMachine, RaftConfig raftConfig) {
        this.groupId = groupId;
        this.raftNode = raftNode;
        this.stateMachine = stateMachine;
        this.raftConfig = raftConfig;
        this.metrics = MetricsRegistry.getInstance().getOrCreateMetrics("RaftGroup-" + groupId);

        // Set leadership change callback
        raftNode.setLeadershipChangeCallback(this::handleLeadershipChange);
    }

    /**
     * Initializes this group.
     *
     * @return A result indicating success or failure
     */
    public Result<Void> initialize() {
        try {
            LOGGER.info("Initializing Raft group " + groupId);

            // Initialize Raft node
            raftNode.initialize();

            return Result.success(null);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to initialize Raft group " + groupId, e);
            return Result.failure("Failed to initialize Raft group: " + e.getMessage());
        }
    }

    /**
     * Starts this group.
     *
     * @return A result indicating success or failure
     */
    public Result<Void> start() {
        if (running.compareAndSet(false, true)) {
            try {
                LOGGER.info("Starting Raft group " + groupId);

                // Start Raft node
                raftNode.start();

                return Result.success(null);
            } catch (Exception e) {
                running.set(false);
                LOGGER.log(Level.SEVERE, "Failed to start Raft group " + groupId, e);
                return Result.failure("Failed to start Raft group: " + e.getMessage());
            }
        }

        return Result.success(null);
    }

    /**
     * Stops this group.
     *
     * @return A result indicating success or failure
     */
    public Result<Void> stop() {
        if (running.compareAndSet(true, false)) {
            try {
                LOGGER.info("Stopping Raft group " + groupId);

                // Stop Raft node
                raftNode.stop();

                // Close state machine
                stateMachine.close();

                return Result.success(null);
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Failed to stop Raft group " + groupId, e);
                return Result.failure("Failed to stop Raft group: " + e.getMessage());
            }
        }

        return Result.success(null);
    }

    /**
     * Handles a leadership change.
     *
     * @param isLeader Whether this node is now the leader
     */
    private void handleLeadershipChange(boolean isLeader) {
        if (isLeader) {
            LOGGER.info("Became leader of group " + groupId);
            leaderId.set(raftNode.getNodeId());
            metrics.counter("leadership.gained").inc();
        } else {
            LOGGER.info("Lost leadership of group " + groupId);
            metrics.counter("leadership.lost").inc();
        }
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
                    Result.failure("Group is not running"));
        }

        metrics.counter("put").inc();

        try {
            // Create command
            byte[] command = KVStateMachine.createDeleteCommand(key);

            // Append to log
            return raftNode.appendEntry(command)
                    .thenApply(result -> {
                        boolean deleted = Boolean.parseBoolean(new String(result));
                        metrics.counter("delete.success").inc();
                        return Result.success(deleted);
                    })
                    .exceptionally(ex -> {
                        metrics.counter("delete.failure").inc();
                        LOGGER.log(Level.WARNING, "Failed to delete key", ex);
                        return Result.failure("Failed to delete key: " + ex.getMessage());
                    });
        } catch (Exception e) {
            metrics.counter("delete.error").inc();
            LOGGER.log(Level.WARNING, "Error creating delete command", e);
            return CompletableFuture.completedFuture(
                    Result.failure("Error creating delete command: " + e.getMessage()));
        }
    }

    /**
     * Creates a client session.
     *
     * @return The session ID
     */
    public String createClientSession() {
        String sessionId = raftNode.createClientSession();
        clientSessions.put(sessionId, new ClientSession(sessionId));
        return sessionId;
    }

    /**
     * Closes a client session.
     *
     * @param sessionId The session ID
     */
    public void closeClientSession(String sessionId) {
        clientSessions.remove(sessionId);
        raftNode.closeClientSession(sessionId);
    }

    /**
     * Gets the status of this group.
     *
     * @return The status
     */
    public Status getStatus() {
        return raftNode.getStatus();
    }

    /**
     * Gets the ID of this group.
     *
     * @return The group ID
     */
    public String getGroupId() {
        return groupId;
    }

    /**
     * Gets the ID of the current leader node.
     *
     * @return The leader ID, or null if unknown
     */
    public String getLeaderId() {
        return leaderId.get();
    }

    /**
     * Checks if this node is the leader of the group.
     *
     * @return True if this node is the leader
     */
    public boolean isLeader() {
        return raftNode.isLeader();
    }

    /**
     * Takes a snapshot of the state machine.
     *
     * @return A result containing the snapshot or an error
     */
    public Result<Snapshot> takeSnapshot() {
        if (!running.get()) {
            return Result.failure("Group is not running");
        }

        try {
            Snapshot snapshot = stateMachine.takeSnapshot();
            return Result.success(snapshot);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to take snapshot", e);
            return Result.failure("Failed to take snapshot: " + e.getMessage());
        }
    }

    /**
     * Represents a client session.
     */
    private static class ClientSession {
        /** The session ID */
        private final String sessionId;

        /** The time when the session was created */
        private final long createTime;

        /** The time of the last activity */
        private volatile long lastActivityTime;

        /**
         * Creates a new client session.
         *
         * @param sessionId The session ID
         */
        public ClientSession(String sessionId) {
            this.sessionId = sessionId;
            this.createTime = System.currentTimeMillis();
            this.lastActivityTime = this.createTime;
        }

        /**
         * Updates the last activity time.
         */
        public void updateLastActivityTime() {
            this.lastActivityTime = System.currentTimeMillis();
        }

        /**
         * Gets the session ID.
         *
         * @return The session ID
         */
        public String getSessionId() {
            return sessionId;
        }

        /**
         * Gets the time when the session was created.
         *
         * @return The create time
         */
        public long getCreateTime() {
            return createTime;
        }

        /**
         * Gets the time of the last activity.
         *
         * @return The last activity time
         */
        public long getLastActivityTime() {
            return lastActivityTime;
        }

        /**
         * Checks if the session is expired.
         *
         * @param timeoutMs The timeout in milliseconds
         * @return True if the session is expired
         */
        public boolean isExpired(long timeoutMs) {
            return System.currentTimeMillis() - lastActivityTime > timeoutMs;
        }
    }
}.createPutCommand(key, value);

            // Append to log
            return raftNode.appendEntry(command)
                    .thenApply(result -> {
                        metrics.counter("put.success").inc();
                        return Result.success(null);
                    })
                    .exceptionally(ex -> {
                        metrics.counter("put.failure").inc();
                        LOGGER.log(Level.WARNING, "Failed to put key", ex);
                        return Result.failure("Failed to put key: " + ex.getMessage());
                    });
        } catch (Exception e) {
            metrics.counter("put.error").inc();
            LOGGER.log(Level.WARNING, "Error creating put command", e);
            return CompletableFuture.completedFuture(
                    Result.failure("Error creating put command: " + e.getMessage()));
        }
    }

    /**
     * Gets the value for a key using the ReadIndex protocol.
     *
     * @param key The key
     * @return A future that completes with a result containing the value or an error
     */
    public CompletableFuture<Result<byte[]>> get(byte[] key) {
        if (!running.get()) {
            return CompletableFuture.completedFuture(
                    Result.failure("Group is not running"));
        }

        metrics.counter("get").inc();

        return raftNode.readWithReadIndex(key)
                .thenApply(value -> {
                    if (value == null) {
                        metrics.counter("get.notfound").inc();
                        return Result.success(null);
                    } else {
                        metrics.counter("get.success").inc();
                        return Result.success(value);
                    }
                })
                .exceptionally(ex -> {
                    metrics.counter("get.failure").inc();
                    LOGGER.log(Level.WARNING, "Failed to get key", ex);
                    return Result.failure("Failed to get key: " + ex.getMessage());
                });
    }

    /**
     * Gets the value for a key using the FollowerRead protocol.
     *
     * @param key The key
     * @return A future that completes with a result containing the value or an error
     */
    public CompletableFuture<Result<byte[]>> getWithFollowerRead(byte[] key) {
        if (!running.get()) {
            return CompletableFuture.completedFuture(
                    Result.failure("Group is not running"));
        }

        metrics.counter("get.followerread").inc();

        return raftNode.readWithFollowerRead(key)
                .thenApply(value -> {
                    if (value == null) {
                        metrics.counter("get.followerread.notfound").inc();
                        return Result.success(null);
                    } else {
                        metrics.counter("get.followerread.success").inc();
                        return Result.success(value);
                    }
                })
                .exceptionally(ex -> {
                    metrics.counter("get.followerread.failure").inc();
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
                    Result.failure("Group is not running"));
        }

        metrics.counter("delete").inc();

        try {
            // Create command
            byte[] command = KVStateMachine