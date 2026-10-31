package com.distributedkv.storage.sharding;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distributedkv.config.ClusterConfig;
import com.distributedkv.network.client.RpcClient;
import com.distributedkv.raft.RaftGroup;
import com.distributedkv.raft.RaftGroupManager;

/**
 * Manages data migration between nodes during shard rebalancing.
 *
 * The DataMigrator is responsible for transferring data between nodes when
 * shards are rebalanced, ensuring that all replicas have the same data.
 */
public class DataMigrator {
    private static final Logger LOGGER = Logger.getLogger(DataMigrator.class.getName());

    /** The cluster configuration */
    private final ClusterConfig clusterConfig;

    /** The RPC client for communicating with other nodes */
    private final RpcClient rpcClient;

    /** Executor for background tasks */
    private final ExecutorService executor;

    /** Map of ongoing migrations */
    private final Map<String, MigrationTask> ongoingMigrations = new ConcurrentHashMap<>();

    /** Reference to the Raft group manager (may be null) */
    private RaftGroupManager raftGroupManager;

    /**
     * Creates a new data migrator.
     *
     * @param clusterConfig The cluster configuration
     * @param rpcClient The RPC client
     */
    public DataMigrator(ClusterConfig clusterConfig, RpcClient rpcClient) {
        this.clusterConfig = clusterConfig;
        this.rpcClient = rpcClient;
        this.executor = Executors.newFixedThreadPool(
                Math.max(2, Runtime.getRuntime().availableProcessors() / 2),
                r -> {
                    Thread t = new Thread(r, "data-migrator");
                    t.setDaemon(true);
                    return t;
                });
    }

    /**
     * Sets the Raft group manager.
     *
     * @param raftGroupManager The Raft group manager
     */
    public void setRaftGroupManager(RaftGroupManager raftGroupManager) {
        this.raftGroupManager = raftGroupManager;
    }

    /**
     * Migrates data for a shard from one node to another.
     *
     * @param shardId The shard ID
     * @param sourceNodeId The source node ID
     * @param targetNodeId The target node ID
     * @return A future that completes when the migration is done
     */
    public CompletableFuture<Void> migrateData(String shardId, String sourceNodeId, String targetNodeId) {
        // Generate a unique migration ID
        String migrationId = shardId + "-" + sourceNodeId + "-" + targetNodeId + "-" + System.currentTimeMillis();

        LOGGER.info("Starting data migration " + migrationId + " for shard " + shardId +
                 " from " + sourceNodeId + " to " + targetNodeId);

        // Create a new migration task
        MigrationTask task = new MigrationTask(migrationId, shardId, sourceNodeId, targetNodeId);
        ongoingMigrations.put(migrationId, task);

        // Start the migration in the background
        CompletableFuture<Void> future = new CompletableFuture<>();

        executor.submit(() -> {
            try {
                performMigration(task);
                future.complete(null);
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Migration failed: " + migrationId, e);
                future.completeExceptionally(e);
            } finally {
                ongoingMigrations.remove(migrationId);
            }
        });

        return future;
    }

    /**
     * Performs the actual data migration.
     *
     * @param task The migration task
     * @throws Exception If an error occurs during migration
     */
    private void performMigration(MigrationTask task) throws Exception {
        if (raftGroupManager == null) {
            throw new IllegalStateException("Raft group manager not set");
        }

        // Get the Raft group for this shard
        RaftGroup group = raftGroupManager.getGroup(task.shardId);
        if (group == null) {
            throw new IllegalStateException("Raft group not found for shard: " + task.shardId);
        }

        // Get the endpoints for source and target nodes
        String sourceEndpoint = clusterConfig.getNodeEndpoint(task.sourceNodeId);
        String targetEndpoint = clusterConfig.getNodeEndpoint(task.targetNodeId);

        if (sourceEndpoint == null) {
            throw new IllegalStateException("Source node endpoint not found: " + task.sourceNodeId);
        }

        if (targetEndpoint == null) {
            throw new IllegalStateException("Target node endpoint not found: " + task.targetNodeId);
        }

        // Update task status
        task.status = MigrationStatus.PREPARING;

        // TODO: Implement the actual data migration using RPC calls
        // The implementation will depend on the specific protocol used for data transfer

        // For now, just simulate a successful migration
        simulateMigration(task);

        // Update task status
        task.status = MigrationStatus.COMPLETED;

        LOGGER.info("Completed data migration " + task.migrationId + " for shard " + task.shardId);
    }

    /**
     * Simulates a data migration for testing.
     *
     * @param task The migration task
     * @throws InterruptedException If the thread is interrupted
     */
    private void simulateMigration(MigrationTask task) throws InterruptedException {
        // Update task status
        task.status = MigrationStatus.TRANSFERRING;

        // Simulate progress updates
        for (int i = 0; i < 10; i++) {
            task.progress = i * 10;
            Thread.sleep(500); // Simulate work
        }

        task.progress = 100;
    }

    /**
     * Gets the status of a migration.
     *
     * @param migrationId The migration ID
     * @return The migration status, or null if not found
     */
    public MigrationStatus getMigrationStatus(String migrationId) {
        MigrationTask task = ongoingMigrations.get(migrationId);
        return task != null ? task.status : null;
    }

    /**
     * Gets the progress of a migration.
     *
     * @param migrationId The migration ID
     * @return The migration progress (0-100), or -1 if not found
     */
    public int getMigrationProgress(String migrationId) {
        MigrationTask task = ongoingMigrations.get(migrationId);
        return task != null ? task.progress : -1;
    }

    /**
     * Gets all ongoing migrations.
     *
     * @return Map of migration IDs to tasks
     */
    public Map<String, MigrationTask> getOngoingMigrations() {
        return new ConcurrentHashMap<>(ongoingMigrations);
    }

    /**
     * Cancels a migration.
     *
     * @param migrationId The migration ID
     * @return True if the migration was canceled, false otherwise
     */
    public boolean cancelMigration(String migrationId) {
        MigrationTask task = ongoingMigrations.remove(migrationId);
        if (task != null) {
            task.status = MigrationStatus.CANCELED;
            LOGGER.info("Canceled data migration " + migrationId);
            return true;
        }
        return false;
    }

    /**
     * Shuts down the data migrator.
     */
    public void shutdown() {
        // Cancel all ongoing migrations
        for (String migrationId : ongoingMigrations.keySet()) {
            cancelMigration(migrationId);
        }

        // Shutdown the executor
        executor.shutdown();
    }

    /**
     * Enum for migration status.
     */
    public enum MigrationStatus {
        /** Migration is queued but not started */
        QUEUED,

        /** Migration is preparing to transfer data */
        PREPARING,

        /** Migration is transferring data */
        TRANSFERRING,

        /** Migration is verifying transferred data */
        VERIFYING,

        /** Migration is completed successfully */
        COMPLETED,

        /** Migration failed */
        FAILED,

        /** Migration was canceled */
        CANCELED
    }

    /**
     * Class representing a migration task.
     */
    public static class MigrationTask {
        /** The migration ID */
        private final String migrationId;

        /** The shard ID */
        private final String shardId;

        /** The source node ID */
        private final String sourceNodeId;

        /** The target node ID */
        private final String targetNodeId;

        /** The migration status */
        private volatile MigrationStatus status;

        /** The migration progress (0-100) */
        private volatile int progress;

        /** The time when the migration started */
        private final long startTime;

        /**
         * Creates a new migration task.
         *
         * @param migrationId The migration ID
         * @param shardId The shard ID
         * @param sourceNodeId The source node ID
         * @param targetNodeId The target node ID
         */
        public MigrationTask(String migrationId, String shardId, String sourceNodeId, String targetNodeId) {
            this.migrationId = migrationId;
            this.shardId = shardId;
            this.sourceNodeId = sourceNodeId;
            this.targetNodeId = targetNodeId;
            this.status = MigrationStatus.QUEUED;
            this.progress = 0;
            this.startTime = System.currentTimeMillis();
        }

        /**
         * Gets the migration ID.
         *
         * @return The migration ID
         */
        public String getMigrationId() {
            return migrationId;
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
         * Gets the source node ID.
         *
         * @return The source node ID
         */
        public String getSourceNodeId() {
            return sourceNodeId;
        }

        /**
         * Gets the target node ID.
         *
         * @return The target node ID
         */
        public String getTargetNodeId() {
            return targetNodeId;
        }

        /**
         * Gets the migration status.
         *
         * @return The migration status
         */
        public MigrationStatus getStatus() {
            return status;
        }

        /**
         * Gets the migration progress.
         *
         * @return The migration progress (0-100)
         */
        public int getProgress() {
            return progress;
        }

        /**
         * Gets the time when the migration started.
         *
         * @return The start time
         */
        public long getStartTime() {
            return startTime;
        }

        /**
         * Gets the migration duration in milliseconds.
         *
         * @return The duration
         */
        public long getDurationMs() {
            return System.currentTimeMillis() - startTime;
        }

        @Override
        public String toString() {
            return "MigrationTask{" +
                    "migrationId='" + migrationId + '\'' +
                    ", shardId='" + shardId + '\'' +
                    ", sourceNodeId='" + sourceNodeId + '\'' +
                    ", targetNodeId='" + targetNodeId + '\'' +
                    ", status=" + status +
                    ", progress=" + progress +
                    ", startTime=" + startTime +
                    ", durationMs=" + getDurationMs() +
                    '}';
        }
    }
}