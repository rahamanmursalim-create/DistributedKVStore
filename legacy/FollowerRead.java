package com.distributedkv.raft;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distributedkv.common.Metrics;
import com.distributedkv.common.MetricsRegistry;
import com.distributedkv.network.protocol.FollowerReadRequestMessage;
import com.distributedkv.network.protocol.FollowerReadResponseMessage;

/**
 * Implementation of the FollowerRead optimization for serving read requests.
 *
 * FollowerRead allows followers to serve read requests directly without
 * consulting the leader in certain scenarios, which can significantly
 * reduce read latency (by 42% as mentioned in the requirements).
 */
public class FollowerRead {
    private static final Logger LOGGER = Logger.getLogger(FollowerRead.class.getName());

    /** The Raft node that this FollowerRead is associated with */
    private final RaftNode raftNode;

    /** The async applier used to wait for entries to be applied */
    private final AsyncApplier asyncApplier;

    /** The read index implementation for fallback */
    private final ReadIndex readIndex;

    /** Map of pending read requests */
    private final Map<String, PendingRead> pendingReads = new ConcurrentHashMap<>();

    /** Executor for background tasks */
    private final ExecutorService executor;

    /** Function to forward follower read requests to the leader */
    private final Function<FollowerReadRequestMessage, CompletableFuture<FollowerReadResponseMessage>> leaderForwarder;

    /** Metrics for monitoring follower read performance */
    private final Metrics metrics;

    /** Timeout for follower read requests (in milliseconds) */
    private final long followerReadTimeoutMs;

    /** Maximum allowed staleness for reads (in milliseconds) */
    private final long maxStalenessMs;

    /** The timestamp of the last heartbeat received from the leader */
    private final AtomicLong lastHeartbeatTime = new AtomicLong(0);

    /** The term of the last heartbeat received from the leader */
    private final AtomicLong lastHeartbeatTerm = new AtomicLong(0);

    /** The commit index in the last heartbeat */
    private final AtomicLong lastHeartbeatCommitIndex = new AtomicLong(0);

    /**
     * Creates a new FollowerRead.
     *
     * @param raftNode The Raft node
     * @param asyncApplier The async applier
     * @param readIndex The read index implementation
     * @param leaderForwarder Function to forward follower read requests to the leader
     * @param followerReadTimeoutMs Timeout for follower read requests
     * @param maxStalenessMs Maximum allowed staleness for reads
     */
    public FollowerRead(RaftNode raftNode, AsyncApplier asyncApplier, ReadIndex readIndex,
                       Function<FollowerReadRequestMessage, CompletableFuture<FollowerReadResponseMessage>> leaderForwarder,
                       long followerReadTimeoutMs, long maxStalenessMs) {
        this.raftNode = raftNode;
        this.asyncApplier = asyncApplier;
        this.readIndex = readIndex;
        this.leaderForwarder = leaderForwarder;
        this.followerReadTimeoutMs = followerReadTimeoutMs;
        this.maxStalenessMs = maxStalenessMs;
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "follower-read-cleaner");
            t.setDaemon(true);
            return t;
        });
        this.metrics = MetricsRegistry.getInstance().getOrCreateMetrics("FollowerRead");

        // Start a background task to clean up timed-out read requests
        startCleanupTask();
    }

    /**
     * Starts a background task to clean up timed-out read requests.
     */
    private void startCleanupTask() {
        ((java.util.concurrent.ScheduledExecutorService) executor).scheduleAtFixedRate(() -> {
            try {
                long now = System.currentTimeMillis();
                pendingReads.entrySet().removeIf(entry -> {
                    PendingRead read = entry.getValue();
                    if (now - read.startTime > followerReadTimeoutMs) {
                        read.future.completeExceptionally(
                                new java.util.concurrent.TimeoutException("Follower read request timed out"));
                        metrics.counter("request.timeout").inc();
                        return true;
                    }
                    return false;
                });
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Error in follower read cleanup task", e);
            }
        }, followerReadTimeoutMs / 2, followerReadTimeoutMs / 2, TimeUnit.MILLISECONDS);
    }

    /**
     * Updates the heartbeat information.
     *
     * @param term The term of the heartbeat
     * @param commitIndex The commit index in the heartbeat
     */
    public void updateHeartbeat(long term, long commitIndex) {
        lastHeartbeatTime.set(System.currentTimeMillis());
        lastHeartbeatTerm.set(term);
        lastHeartbeatCommitIndex.set(commitIndex);
    }

    /**
     * Checks if follower read is possible.
     *
     * @return True if follower read is possible, false otherwise
     */
    public boolean isFollowerReadPossible() {
        // Follower read is possible if:
        // 1. We are a follower
        // 2. We have received a heartbeat recently
        // 3. We are in the same term as the last heartbeat

        if (raftNode.isLeader()) {
            return false;
        }

        long currentTime = System.currentTimeMillis();
        long heartbeatTime = lastHeartbeatTime.get();
        long heartbeatTerm = lastHeartbeatTerm.get();

        return heartbeatTime > 0 &&
               currentTime - heartbeatTime <= maxStalenessMs &&
               heartbeatTerm == raftNode.getCurrentTerm();
    }

    /**
     * Processes a read request.
     *
     * @param key The key to read (may be null for non-key-specific reads)
     * @return A future that will complete with the read index
     */
    public CompletableFuture<Long> processRead(byte[] key) {
        metrics.counter("request").inc();
        long startTime = System.currentTimeMillis();

        // Check if follower read is possible
        if (!isFollowerReadPossible()) {
            // Fall back to ReadIndex
            metrics.counter("fallback.readindex").inc();
            return readIndex.processRead(key);
        }

        // Generate a unique read ID
        String readId = UUID.randomUUID().toString();

        // Create a pending read
        PendingRead pendingRead = new PendingRead(readId, startTime);
        pendingReads.put(readId, pendingRead);

        // Get the current state
        long currentTerm = raftNode.getCurrentTerm();
        long lastApplied = asyncApplier.getLastAppliedIndex();
        long readTimestamp = System.currentTimeMillis();

        // Create a follower read request
        FollowerReadRequestMessage request = new FollowerReadRequestMessage(
                currentTerm, raftNode.getGroupId(), raftNode.getNodeId(),
                readId, key, readTimestamp, lastApplied);

        // Forward the request to the leader
        leaderForwarder.apply(request)
                .thenAccept(response -> {
                    if (response.isSafeToRead()) {
                        if (response.hasRequiredIndex()) {
                            // We need to wait until we've applied up to the required index
                            metrics.counter("wait_for_index").inc();
                            pendingRead.future.complete(response.getRequiredAppliedIndex());
                        } else {
                            // It's safe to read immediately using our last applied index
                            metrics.counter("immediate_read").inc();
                            pendingRead.future.complete(lastApplied);
                        }
                    } else if (response.hasError()) {
                        // Leader reported an error
                        pendingRead.future.completeExceptionally(
                                new RuntimeException("Follower read failed: " + response.getErrorMessage()));
                        metrics.counter("error").inc();
                    } else {
                        // Not safe to read, fall back to ReadIndex
                        metrics.counter("fallback.unsafe").inc();
                        readIndex.processRead(key)
                                .thenAccept(pendingRead.future::complete)
                                .exceptionally(ex -> {
                                    pendingRead.future.completeExceptionally(ex);
                                    return null;
                                });
                    }

                    pendingReads.remove(pendingRead.readId);
                })
                .exceptionally(ex -> {
                    pendingRead.future.completeExceptionally(ex);
                    pendingReads.remove(pendingRead.readId);
                    metrics.counter("request.exception").inc();
                    return null;
                });

        return pendingRead.future.thenApply(readIndex -> {
            metrics.histogram("latency").update(System.currentTimeMillis() - startTime);
            return readIndex;
        });
    }

    /**
     * Performs a read operation using follower read if possible.
     *
     * @param key The key to read
     * @param readOperation The read operation to perform
     * @param <T> The type of the result
     * @return A future that will complete with the result of the read
     */
    public <T> CompletableFuture<T> read(byte[] key, Function<byte[], T> readOperation) {
        return processRead(key)
                .thenCompose(readIndex -> asyncApplier.waitForApplied(readIndex))
                .thenApply(v -> readOperation.apply(key));
    }

    /**
     * Processes a follower read request (called when this node is the leader).
     *
     * @param request The follower read request
     * @return The follower read response
     */
    public FollowerReadResponseMessage processFollowerReadRequest(FollowerReadRequestMessage request) {
        metrics.counter("process.request").inc();

        if (!raftNode.isLeader()) {
            metrics.counter("process.not_leader").inc();
            return new FollowerReadResponseMessage(
                    raftNode.getCurrentTerm(), raftNode.getGroupId(), raftNode.getNodeId(),
                    request.getReadId(), "Not the leader");
        }

        try {
            // Check if the follower is in the current term
            if (request.getTerm() != raftNode.getCurrentTerm()) {
                metrics.counter("process.term_mismatch").inc();
                return new FollowerReadResponseMessage(
                        raftNode.getCurrentTerm(), raftNode.getGroupId(), raftNode.getNodeId(),
                        request.getReadId(), "Term mismatch");
            }

            // Get the current commit index
            long commitIndex = raftNode.getCommitIndex();

            // Check if the follower is caught up
            long followerApplied = request.getLastAppliedIndex();

            if (followerApplied >= commitIndex) {
                // Follower is caught up, safe to read immediately
                metrics.counter("process.immediate_read").inc();
                return new FollowerReadResponseMessage(
                        raftNode.getCurrentTerm(), raftNode.getGroupId(), raftNode.getNodeId(),
                        request.getReadId());
            } else {
                // Follower needs to catch up to the commit index
                metrics.counter("process.wait_for_index").inc();
                return new FollowerReadResponseMessage(
                        raftNode.getCurrentTerm(), raftNode.getGroupId(), raftNode.getNodeId(),
                        request.getReadId(), commitIndex);
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error processing follower read request", e);
            metrics.counter("process.error").inc();
            return new FollowerReadResponseMessage(
                    raftNode.getCurrentTerm(), raftNode.getGroupId(), raftNode.getNodeId(),
                    request.getReadId(), "Error: " + e.getMessage());
        }
    }

    /**
     * Stops the FollowerRead.
     */
    public void stop() {
        executor.shutdown();

        // Complete all pending reads with an exception
        pendingReads.forEach((readId, pendingRead) -> {
            pendingRead.future.completeExceptionally(
                    new IllegalStateException("FollowerRead stopped"));
        });
        pendingReads.clear();
    }

    /**
     * Represents a pending read request.
     */
    private static class PendingRead {
        final String readId;
        final long startTime;
        final CompletableFuture<Long> future;

        PendingRead(String readId, long startTime) {
            this.readId = readId;
            this.startTime = startTime;
            this.future = new CompletableFuture<>();
        }
    }
}
