package com.distributedkv.raft.election;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distributedkv.common.Metrics;
import com.distributedkv.common.MetricsRegistry;
import com.distributedkv.raft.Log;
import com.distributedkv.raft.LogEntry;
import com.distributedkv.raft.RaftPeer;
import com.distributedkv.network.protocol.AppendEntriesRequestMessage;
import com.distributedkv.network.protocol.AppendEntriesResponseMessage;

/**
 * Manages state and behavior specific to the leader role in the Raft protocol.
 *
 * This class is responsible for log replication, heartbeat scheduling, and
 * commit index management when a node is in the leader state.
 */
public class LeaderState {
    private static final Logger LOGGER = Logger.getLogger(LeaderState.class.getName());

    /** The current term */
    private final long term;

    /** The node ID */
    private final String nodeId;

    /** The group ID */
    private final String groupId;

    /** The Raft log */
    private final Log log;

    /** The peers in the Raft group */
    private final Map<String, RaftPeer> peers;

    /** The executor for scheduling tasks */
    private final ScheduledExecutorService executor;

    /** The heartbeat interval in milliseconds */
    private final int heartbeatIntervalMs;

    /** Lock to ensure thread safety */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** The scheduled heartbeat task */
    private volatile ScheduledFuture<?> heartbeatTask;

    /** Flag indicating whether this leader state is active */
    private final AtomicBoolean active = new AtomicBoolean(false);

    /** The time when this leader state was created */
    private final long startTimeMs;

    /** Map of client request futures by log index */
    private final Map<Long, CompletableFuture<byte[]>> pendingRequests = new HashMap<>();

    /** Metrics for monitoring leader performance */
    private final Metrics metrics;

    /** Callback for commit index updates */
    private Runnable commitIndexUpdateCallback;

    /**
     * Creates a new leader state.
     *
     * @param term The current term
     * @param nodeId The node ID
     * @param groupId The group ID
     * @param log The Raft log
     * @param peers The peers in the Raft group
     * @param executor The executor
     * @param heartbeatIntervalMs The heartbeat interval
     */
    public LeaderState(long term, String nodeId, String groupId, Log log,
                      Map<String, RaftPeer> peers, ScheduledExecutorService executor,
                      int heartbeatIntervalMs) {
        this.term = term;
        this.nodeId = nodeId;
        this.groupId = groupId;
        this.log = log;
        this.peers = new HashMap<>(peers);
        this.executor = executor;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.startTimeMs = System.currentTimeMillis();
        this.metrics = MetricsRegistry.getInstance().getOrCreateMetrics("LeaderState");
    }

    /**
     * Sets the callback for commit index updates.
     *
     * @param callback The callback
     */
    public void setCommitIndexUpdateCallback(Runnable callback) {
        this.commitIndexUpdateCallback = callback;
    }

    /**
     * Starts this leader state.
     */
    public void start() {
        if (active.compareAndSet(false, true)) {
            LOGGER.info("Starting leader state for term " + term);

            try {
                // Append a no-op entry to the log to ensure all previous entries are committed
                LogEntry noOpEntry = log.append(term, null, LogEntry.EntryType.NO_OP);
                LOGGER.info("Appended no-op entry: " + noOpEntry);

                // Initialize nextIndex and matchIndex for all peers
                long lastLogIndex = log.getLastIndex();
                peers.values().forEach(peer -> {
                    peer.setNextIndex(lastLogIndex + 1);
                    // Note: matchIndex is initialized to 0 in RaftPeer constructor
                });

                // Schedule heartbeat task
                scheduleHeartbeat();

                // Send initial heartbeats
                sendHeartbeats();
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Failed to start leader state", e);
                active.set(false);
                throw new RuntimeException("Failed to start leader state", e);
            }
        }
    }

    /**
     * Stops this leader state.
     */
    public void stop() {
        if (active.compareAndSet(true, false)) {
            LOGGER.info("Stopping leader state for term " + term);

            // Cancel heartbeat task
            if (heartbeatTask != null) {
                heartbeatTask.cancel(false);
                heartbeatTask = null;
            }

            // Complete pending requests with an exception
            lock.writeLock().lock();
            try {
                pendingRequests.forEach((index, future) -> {
                    future.completeExceptionally(
                            new IllegalStateException("Leader stepped down"));
                });
                pendingRequests.clear();
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    /**
     * Schedules the heartbeat task.
     */
    private void scheduleHeartbeat() {
        heartbeatTask = executor.scheduleAtFixedRate(
                this::sendHeartbeats,
                0,
                heartbeatIntervalMs,
                TimeUnit.MILLISECONDS);
    }

    /**
     * Sends heartbeats to all peers.
     */
    public void sendHeartbeats() {
        if (!active.get()) {
            return;
        }

        metrics.counter("heartbeat").inc();

        // For each peer, send an AppendEntries RPC
        peers.values().forEach(this::replicateLogToPeer);

        // Update commit index
        updateCommitIndex();
    }

    /**
     * Broadcasts heartbeats to all peers and waits for a majority to respond.
     *
     * @return A future that completes with true if a majority acknowledged
     */
    public CompletableFuture<Boolean> broadcastHeartbeats() {
        if (!active.get()) {
            return CompletableFuture.completedFuture(false);
        }

        metrics.counter("broadcast.heartbeat").inc();

        // Create a future for each peer
        Map<String, CompletableFuture<Boolean>> peerFutures = new HashMap<>();

        // Send heartbeats to all peers
        peers.forEach((id, peer) -> {
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            peerFutures.put(id, future);

            replicateLogToPeer(peer)
                    .thenAccept(response -> {
                        boolean success = response.isSuccess() &&
                                         response.getTerm() == term;
                        future.complete(success);
                    })
                    .exceptionally(ex -> {
                        future.complete(false);
                        return null;
                    });
        });

        // Wait for a majority of acknowledgments
        int requiredAcks = (peers.size() / 2) + 1;

        // Count the local node as an ack
        AtomicBoolean result = new AtomicBoolean(false);
        AtomicBoolean completed = new AtomicBoolean(false);

        // Create a combined future
        CompletableFuture<Boolean> combinedFuture = new CompletableFuture<>();

        // Check if we already have enough acks
        checkAcks(peerFutures, requiredAcks, result, completed, combinedFuture);

        // Wait for all peer futures to complete
        CompletableFuture.allOf(peerFutures.values().toArray(new CompletableFuture[0]))
                .whenComplete((v, ex) -> {
                    if (!completed.get()) {
                        // Count the acks
                        int acks = 1; // Count the local node
                        for (CompletableFuture<Boolean> future : peerFutures.values()) {
                            try {
                                if (future.isDone() && future.get()) {
                                    acks++;
                                }
                            } catch (Exception e) {
                                // Ignore exceptions, they're treated as non-acks
                            }
                        }

                        result.set(acks > requiredAcks);
                        completed.set(true);
                        combinedFuture.complete(result.get());
                    }
                });

        return combinedFuture;
    }

    /**
     * Checks if we have enough acknowledgments to complete the heartbeat broadcast.
     */
    private void checkAcks(Map<String, CompletableFuture<Boolean>> peerFutures,
                          int requiredAcks, AtomicBoolean result,
                          AtomicBoolean completed, CompletableFuture<Boolean> combinedFuture) {
        if (completed.get()) {
            return;
        }

        int acks = 1; // Count the local node
        int pending = 0;

        for (CompletableFuture<Boolean> future : peerFutures.values()) {
            if (future.isDone()) {
                try {
                    if (future.get()) {
                        acks++;
                    }
                } catch (Exception e) {
                    // Ignore exceptions, they're treated as non-acks
                }
            } else {
                pending++;
            }
        }

        if (acks >= requiredAcks) {
            // We have enough acks
            result.set(true);
            completed.set(true);
            combinedFuture.complete(true);
        } else if (pending == 0 || acks + pending < requiredAcks) {
            // We can't possibly get enough acks
            result.set(false);
            completed.set(true);
            combinedFuture.complete(false);
        } else {
            // We need to wait for more futures to complete
            executor.schedule(() -> checkAcks(peerFutures, requiredAcks, result, completed, combinedFuture),
                    10, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Replicates the log to a specific peer.
     *
     * @param peer The peer
     * @return A future that completes with the response
     */
    public CompletableFuture<AppendEntriesResponseMessage> replicateLogToPeer(RaftPeer peer) {
        if (!active.get()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Leader state is not active"));
        }

        metrics.counter("replicate").inc();

        // Determine the next index to send to this peer
        long nextIndex = peer.getNextIndex();
        long prevLogIndex = nextIndex - 1;
        long prevLogTerm = log.getTerm(prevLogIndex);

        // Get entries to send
        List<LogEntry> entries = log.getEntriesFrom(nextIndex);

        // Create AppendEntries request
        AppendEntriesRequestMessage request = new AppendEntriesRequestMessage(
                term, groupId, nodeId, nodeId, prevLogIndex, prevLogTerm,
                entries, log.getCommitIndex());

        // Send request to peer
        return peer.appendEntries(request)
                .thenApply(response -> {
                    metrics.counter("replicate.response").inc();

                    if (response.getTerm() > term) {
                        // The peer has a higher term, we should step down
                        LOGGER.warning("Peer " + peer.getId() + " has higher term: " +
                                     response.getTerm() + " > " + term);
                        metrics.counter("replicate.higher_term").inc();
                        stop();
                    } else if (response.isSuccess()) {
                        // The peer accepted the entries
                        metrics.counter("replicate.success").inc();

                        // Update commit index after successful replication
                        updateCommitIndex();
                    } else {
                        // The peer rejected the entries, optimize nextIndex for the next attempt
                        metrics.counter("replicate.failure").inc();
                        peer.optimizeNextIndex(response);
                    }

                    return response;
                })
                .exceptionally(ex -> {
                    metrics.counter("replicate.error").inc();
                    LOGGER.log(Level.WARNING, "Failed to replicate to peer " + peer.getId(), ex);

                    // Create a failure response
                    return new AppendEntriesResponseMessage(
                            term, groupId, peer.getId(), false, prevLogIndex);
                });
    }

    /**
     * Updates the commit index based on the match indices of all peers.
     */
    private void updateCommitIndex() {
        lock.writeLock().lock();
        try {
            // Get current commit index
            long oldCommitIndex = log.getCommitIndex();

            // Get match indices from all peers
            List<Long> matchIndices = new ArrayList<>(peers.size() + 1);
            matchIndices.add(log.getLastIndex()); // Local node is fully up-to-date
            peers.values().forEach(peer -> matchIndices.add(peer.getMatchIndex()));

            // Sort match indices
            Collections.sort(matchIndices);

            // The commit index is the median match index
            int quorumIndex = matchIndices.size() / 2;
            long newCommitIndex = matchIndices.get(quorumIndex);

            // Only commit entries from the current term
            for (long i = oldCommitIndex + 1; i <= newCommitIndex; i++) {
                if (log.getTerm(i) == term) {
                    // This entry can be committed
                    log.setCommitIndex(i);

                    // Complete the pending request if any
                    CompletableFuture<byte[]> future = pendingRequests.remove(i);
                    if (future != null) {
                        // The request will be completed when the entry is applied
                        // to the state machine by the async applier
                    }
                }
            }

            // Notify about commit index update if needed
            if (log.getCommitIndex() > oldCommitIndex) {
                metrics.counter("commit_index.update").inc();

                if (commitIndexUpdateCallback != null) {
                    try {
                        commitIndexUpdateCallback.run();
                    } catch (Exception e) {
                        LOGGER.log(Level.WARNING, "Error in commit index update callback", e);
                    }
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Appends a new entry to the log and replicates it to all peers.
     *
     * @param command The command to append
     * @return A future that completes with the result of applying the command
     */
    public CompletableFuture<byte[]> appendEntry(byte[] command) {
        if (!active.get()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Leader state is not active"));
        }

        metrics.counter("append").inc();

        try {
            // Append the entry to the log
            LogEntry entry = log.append(term, command, LogEntry.EntryType.NORMAL);

            // Create a future for the result
            CompletableFuture<byte[]> future = new CompletableFuture<>();

            // Store the future
            lock.writeLock().lock();
            try {
                pendingRequests.put(entry.getIndex(), future);
            } finally {
                lock.writeLock().unlock();
            }

            // Replicate the entry to all peers
            sendHeartbeats();

            return future;
        } catch (Exception e) {
            metrics.counter("append.error").inc();
            LOGGER.log(Level.SEVERE, "Failed to append entry", e);
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Gets the current term.
     *
     * @return The term
     */
    public long getTerm() {
        return term;
    }

    /**
     * Gets the time when this leader state was created.
     *
     * @return The start time
     */
    public long getStartTimeMs() {
        return startTimeMs;
    }

    /**
     * Checks if this leader state is active.
     *
     * @return True if active
     */
    public boolean isActive() {
        return active.get();
    }

    /**
     * Gets the number of peers in the Raft group.
     *
     * @return The number of peers
     */
    public int getPeerCount() {
        return peers.size();
    }

    /**
     * Gets a peer by ID.
     *
     * @param peerId The peer ID
     * @return The peer, or null if not found
     */
    public RaftPeer getPeer(String peerId) {
        return peers.get(peerId);
    }

    /**
     * Gets all peers.
     *
     * @return An unmodifiable map of peers
     */
    public Map<String, RaftPeer> getPeers() {
        return Collections.unmodifiableMap(peers);
    }

    /**
     * Completes a pending request with the specified result.
     *
     * @param index The log index
     * @param result The result
     */
    public void completeRequest(long index, byte[] result) {
        lock.writeLock().lock();
        try {
            CompletableFuture<byte[]> future = pendingRequests.remove(index);
            if (future != null) {
                future.complete(result);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Completes a pending request with an exception.
     *
     * @param index The log index
     * @param exception The exception
     */
    public void completeRequestExceptionally(long index, Throwable exception) {
        lock.writeLock().lock();
        try {
            CompletableFuture<byte[]> future = pendingRequests.remove(index);
            if (future != null) {
                future.completeExceptionally(exception);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
}