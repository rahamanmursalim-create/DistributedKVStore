package com.distributedkv.raft;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distributedkv.common.Metrics;
import com.distributedkv.common.MetricsRegistry;
import com.distributedkv.common.util.ConcurrencyUtil;
import com.distributedkv.network.protocol.ReadIndexRequestMessage;
import com.distributedkv.network.protocol.ReadIndexResponseMessage;

/**
 * Implementation of the ReadIndex optimization for linearizable reads.
 *
 * ReadIndex allows followers to serve read requests without forwarding them
 * to the leader, while still maintaining linearizable semantics. This can
 * significantly reduce read latency and leader load.
 */
public class ReadIndex {
    private static final Logger LOGGER = Logger.getLogger(ReadIndex.class.getName());

    /** The Raft node that this ReadIndex is associated with */
    private final RaftNode raftNode;

    /** The async applier used to wait for entries to be applied */
    private final AsyncApplier asyncApplier;

    /** Map of pending read requests */
    private final Map<String, PendingRead> pendingReads = new ConcurrentHashMap<>();

    /** Executor for background tasks */
    private final ExecutorService executor;

    /** Function to forward read index requests to the leader */
    private final Function<ReadIndexRequestMessage, CompletableFuture<ReadIndexResponseMessage>> leaderForwarder;

    /** Metrics for monitoring read index performance */
    private final Metrics metrics;

    /** Timeout for read index requests (in milliseconds) */
    private final long readIndexTimeoutMs;

    /**
     * Creates a new ReadIndex.
     *
     * @param raftNode The Raft node
     * @param asyncApplier The async applier
     * @param leaderForwarder Function to forward read index requests to the leader
     * @param readIndexTimeoutMs Timeout for read index requests
     */
    public ReadIndex(RaftNode raftNode, AsyncApplier asyncApplier,
                    Function<ReadIndexRequestMessage, CompletableFuture<ReadIndexResponseMessage>> leaderForwarder,
                    long readIndexTimeoutMs) {
        this.raftNode = raftNode;
        this.asyncApplier = asyncApplier;
        this.leaderForwarder = leaderForwarder;
        this.readIndexTimeoutMs = readIndexTimeoutMs;
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "read-index-cleaner");
            t.setDaemon(true);
            return t;
        });
        this.metrics = MetricsRegistry.getInstance().getOrCreateMetrics("ReadIndex");

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
                    if (now - read.startTime > readIndexTimeoutMs) {
                        read.future.completeExceptionally(
                                new java.util.concurrent.TimeoutException("Read index request timed out"));
                        metrics.counter("request.timeout").inc();
                        return true;
                    }
                    return false;
                });
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Error in read index cleanup task", e);
            }
        }, readIndexTimeoutMs / 2, readIndexTimeoutMs / 2, TimeUnit.MILLISECONDS);
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

        // Generate a unique read ID
        String readId = UUID.randomUUID().toString();

        // Create a pending read
        PendingRead pendingRead = new PendingRead(readId, startTime);
        pendingReads.put(readId, pendingRead);

        // Handle the read request based on the node's role
        if (raftNode.isLeader()) {
            // Leader can serve reads after a round of heartbeats
            processReadAsLeader(pendingRead);
        } else {
            // Follower must forward the request to the leader
            processReadAsFollower(pendingRead, key);
        }

        return pendingRead.future.thenApply(readIndex -> {
            metrics.histogram("latency").update(System.currentTimeMillis() - startTime);
            return readIndex;
        });
    }

    /**
     * Processes a read request as the leader.
     *
     * @param pendingRead The pending read
     */
    private void processReadAsLeader(PendingRead pendingRead) {
        metrics.counter("leader.request").inc();

        // The leader needs to:
        // 1. Record the current commit index
        // 2. Send heartbeats to confirm leadership
        // 3. Wait for the heartbeats to be acknowledged
        // 4. Return the commit index as the read index

        long commitIndex = raftNode.getCommitIndex();

        // Send heartbeats and wait for replies
        raftNode.broadcastHeartbeats()
                .thenAccept(success -> {
                    if (success) {
                        // Leadership confirmed, complete the read with the recorded commit index
                        pendingRead.future.complete(commitIndex);
                        pendingReads.remove(pendingRead.readId);
                        metrics.counter("leader.success").inc();
                    } else {
                        // Leadership not confirmed
                        pendingRead.future.completeExceptionally(
                                new IllegalStateException("Leadership could not be confirmed"));
                        pendingReads.remove(pendingRead.readId);
                        metrics.counter("leader.failure").inc();
                    }
                })
                .exceptionally(ex -> {
                    pendingRead.future.completeExceptionally(ex);
                    pendingReads.remove(pendingRead.readId);
                    metrics.counter("leader.error").inc();
                    return null;
                });
    }

    /**
     * Processes a read request as a follower.
     *
     * @param pendingRead The pending read
     * @param key The key to read
     */
    private void processReadAsFollower(PendingRead pendingRead, byte[] key) {
        metrics.counter("follower.request").inc();

        // Create a read index request
        ReadIndexRequestMessage request = new ReadIndexRequestMessage(
                raftNode.getCurrentTerm(), raftNode.getGroupId(), raftNode.getNodeId(),
                pendingRead.readId, key);

        // Forward the request to the leader
        leaderForwarder.apply(request)
                .thenAccept(response -> {
                    if (response.isSuccess()) {
                        // Read index obtained from the leader
                        pendingRead.future.complete(response.getReadIndex());
                        pendingReads.remove(pendingRead.readId);
                        metrics.counter("follower.success").inc();
                    } else {
                        // Leader reported an error
                        pendingRead.future.completeExceptionally(
                                new RuntimeException("Read index failed: " + response.getErrorMessage()));
                        pendingReads.remove(pendingRead.readId);
                        metrics.counter("follower.failure").inc();
                    }
                })
                .exceptionally(ex -> {
                    pendingRead.future.completeExceptionally(ex);
                    pendingReads.remove(pendingRead.readId);
                    metrics.counter("follower.error").inc();
                    return null;
                });
    }

    /**
     * Performs a linearizable read operation.
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
     * Processes a read index request (called when this node is the leader).
     *
     * @param request The read index request
     * @return The read index response
     */
    public ReadIndexResponseMessage processReadIndexRequest(ReadIndexRequestMessage request) {
        metrics.counter("process.request").inc();

        if (!raftNode.isLeader()) {
            metrics.counter("process.not_leader").inc();
            return new ReadIndexResponseMessage(
                    raftNode.getCurrentTerm(), raftNode.getGroupId(), raftNode.getNodeId(),
                    request.getReadId(), "Not the leader");
        }

        try {
            // Record the current commit index
            long commitIndex = raftNode.getCommitIndex();

            // Confirm leadership with a round of heartbeats
            CompletableFuture<Boolean> leadershipConfirmed = raftNode.broadcastHeartbeats();

            // Wait for the confirmation
            boolean confirmed = ConcurrencyUtil.getFutureResult(leadershipConfirmed, readIndexTimeoutMs);

            if (confirmed) {
                metrics.counter("process.success").inc();
                return new ReadIndexResponseMessage(
                        raftNode.getCurrentTerm(), raftNode.getGroupId(), raftNode.getNodeId(),
                        request.getReadId(), commitIndex);
            } else {
                metrics.counter("process.leadership_lost").inc();
                return new ReadIndexResponseMessage(
                        raftNode.getCurrentTerm(), raftNode.getGroupId(), raftNode.getNodeId(),
                        request.getReadId(), "Leadership could not be confirmed");
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error processing read index request", e);
            metrics.counter("process.error").inc();
            return new ReadIndexResponseMessage(
                    raftNode.getCurrentTerm(), raftNode.getGroupId(), raftNode.getNodeId(),
                    request.getReadId(), "Error: " + e.getMessage());
        }
    }

    /**
     * Stops the ReadIndex.
     */
    public void stop() {
        executor.shutdown();

        // Complete all pending reads with an exception
        pendingReads.forEach((readId, pendingRead) -> {
            pendingRead.future.completeExceptionally(
                    new IllegalStateException("ReadIndex stopped"));
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