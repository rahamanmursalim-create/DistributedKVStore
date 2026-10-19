package com.distributedkv.raft;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distributedkv.common.Metrics;
import com.distributedkv.common.MetricsRegistry;
import com.distributedkv.common.Status;
import com.distributedkv.config.NodeConfig;
import com.distributedkv.config.RaftConfig;
import com.distributedkv.network.client.RpcClient;
import com.distributedkv.network.protocol.AppendEntriesRequestMessage;
import com.distributedkv.network.protocol.AppendEntriesResponseMessage;
import com.distributedkv.network.protocol.AsyncApplyNotificationMessage;
import com.distributedkv.network.protocol.FollowerReadRequestMessage;
import com.distributedkv.network.protocol.FollowerReadResponseMessage;
import com.distributedkv.network.protocol.InstallSnapshotRequestMessage;
import com.distributedkv.network.protocol.InstallSnapshotResponseMessage;
import com.distributedkv.network.protocol.Message;
import com.distributedkv.network.protocol.ReadIndexRequestMessage;
import com.distributedkv.network.protocol.ReadIndexResponseMessage;
import com.distributedkv.network.protocol.VoteRequestMessage;
import com.distributedkv.network.protocol.VoteResponseMessage;
import com.distributedkv.raft.election.ElectionTimer;
import com.distributedkv.raft.election.LeaderState;

/**
 * Implementation of a node in the Raft consensus protocol.
 *
 * RaftNode is the central component that implements the Raft consensus algorithm.
 * It manages the state machine, log, and communication with other nodes in the
 * Raft group to provide consistent, replicated state.
 */
public class RaftNode {
    private static final Logger LOGGER = Logger.getLogger(RaftNode.class.getName());

    /**
     * Enum representing the possible roles of a Raft node.
     */
    public enum Role {
        FOLLOWER,
        CANDIDATE,
        LEADER
    }

    /** The ID of this node */
    private final String nodeId;

    /** The ID of the Raft group this node belongs to */
    private final String groupId;

    /** The configuration for this node */
    private final NodeConfig nodeConfig;

    /** The Raft-specific configuration */
    private final RaftConfig raftConfig;

    /** The current role of this node */
    private final AtomicReference<Role> role = new AtomicReference<>(Role.FOLLOWER);

    /** The current term */
    private final AtomicLong currentTerm = new AtomicLong(0);

    /** The ID of the node voted for in the current term */
    private final AtomicReference<String> votedFor = new AtomicReference<>(null);

    /** The ID of the current leader */
    private final AtomicReference<String> leaderId = new AtomicReference<>(null);

    /** The Raft log */
    private final Log log;

    /** The state machine */
    private final StateMachine stateMachine;

    /** The storage for Raft state */
    private final RaftStorage storage;

    /** The peers in the Raft group */
    private final Map<String, RaftPeer> peers = new ConcurrentHashMap<>();

    /** Lock to ensure thread safety */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** The election timer */
    private final ElectionTimer electionTimer;

    /** The current leader state (only valid when role is LEADER) */
    private final AtomicReference<LeaderState> leaderState = new AtomicReference<>(null);

    /** Flag indicating whether this node is running */
    private final AtomicBoolean running = new AtomicBoolean(false);

    /** The RPC client for communicating with other nodes */
    private final RpcClient rpcClient;

    /** Executor for background tasks */
    private final ExecutorService executor;

    /** Scheduler for periodic tasks */
    private final ScheduledExecutorService scheduler;

    /** The async applier for log entries */
    private final AsyncApplier asyncApplier;

    /** The ReadIndex implementation */
    private final ReadIndex readIndex;

    /** The FollowerRead implementation */
    private final FollowerRead followerRead;

    /** Callback for leadership changes */
    private Consumer<Boolean> leadershipChangeCallback;

    /** Callback for term changes */
    private Consumer<Long> termChangeCallback;

    /** Callback for role changes */
    private Consumer<Role> roleChangeCallback;

    /** Set of client session IDs */
    private final Set<String> clientSessions = Collections.newSetFromMap(new ConcurrentHashMap<>());

    /** Metrics for monitoring node performance */
    private final Metrics metrics;

    /** The time of the last activity (e.g., heartbeat, vote) */
    private final AtomicLong lastActivityTime = new AtomicLong(0);

    /**
     * Creates a new Raft node.
     *
     * @param nodeId The ID of this node
     * @param groupId The ID of the Raft group
     * @param nodeConfig The node configuration
     * @param raftConfig The Raft configuration
     * @param log The Raft log
     * @param stateMachine The state machine
     * @param storage The Raft storage
     * @param rpcClient The RPC client
     * @param peers The peers in the Raft group
     */
    public RaftNode(String nodeId, String groupId, NodeConfig nodeConfig, RaftConfig raftConfig,
                   Log log, StateMachine stateMachine, RaftStorage storage, RpcClient rpcClient,
                   Map<String, RaftPeer> peers) {
        this.nodeId = nodeId;
        this.groupId = groupId;
        this.nodeConfig = nodeConfig;
        this.raftConfig = raftConfig;
        this.log = log;
        this.stateMachine = stateMachine;
        this.storage = storage;
        this.rpcClient = rpcClient;
        this.metrics = MetricsRegistry.getInstance().getOrCreateMetrics("RaftNode-" + nodeId);

        // Initialize peer map
        if (peers != null) {
            this.peers.putAll(peers);
        }

        // Create executors
        this.executor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "raft-node-" + nodeId);
            t.setDaemon(true);
            return t;
        });

        this.scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "raft-scheduler-" + nodeId);
            t.setDaemon(true);
            return t;
        });

        // Create election timer
        this.electionTimer = new ElectionTimer(
                scheduler,
                raftConfig.getMinElectionTimeoutMs(),
                raftConfig.getMaxElectionTimeoutMs(),
                this::startElection);

        // Create async applier
        this.asyncApplier = new AsyncApplier(
                stateMachine,
                log,
                raftConfig.getMaxBatchSize());

        // Set async applier notification callback
        this.asyncApplier.setNotificationCallback(this::handleAsyncApplyNotification);

        // Create ReadIndex
        this.readIndex = new ReadIndex(
                this,
                asyncApplier,
                this::forwardReadIndexRequest,
                raftConfig.getReadIndexTimeoutMs());

        // Create FollowerRead
        this.followerRead = new FollowerRead(
                this,
                asyncApplier,
                readIndex,
                this::forwardFollowerReadRequest,
                raftConfig.getFollowerReadTimeoutMs(),
                raftConfig.getMaxStalenessMs());
    }

    /**
     * Initializes this node.
     *
     * @throws IOException If an I/O error occurs
     */
    public void initialize() throws IOException {
        LOGGER.info("Initializing Raft node " + nodeId + " in group " + groupId);

        // Initialize storage
        storage.initialize();

        // Load persistent state
        long persistedTerm = storage.readCurrentTerm();
        String persistedVotedFor = storage.readVotedFor();

        LOGGER.info("Loaded persistent state: term=" + persistedTerm +
                  ", votedFor=" + persistedVotedFor);

        // Update in-memory state
        currentTerm.set(persistedTerm);
        votedFor.set(persistedVotedFor);

        // Initialize log
        log.initialize();

        // Reset leader ID
        leaderId.set(null);

        // Set initial role to FOLLOWER
        role.set(Role.FOLLOWER);

        LOGGER.info("Raft node initialized");
    }

    /**
     * Starts this node.
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            LOGGER.info("Starting Raft node " + nodeId);

            // Start async applier
            asyncApplier.start();

            // Start election timer
            electionTimer.start();

            // Schedule snapshot task
            if (raftConfig.getSnapshotIntervalMs() > 0) {
                scheduler.scheduleAtFixedRate(
                        this::checkSnapshotNeeded,
                        raftConfig.getSnapshotIntervalMs(),
                        raftConfig.getSnapshotIntervalMs(),
                        TimeUnit.MILLISECONDS);
            }

            // Schedule log compaction check
            if (raftConfig.getLogCompactionCheckIntervalMs() > 0) {
                scheduler.scheduleAtFixedRate(
                        this::checkLogCompactionNeeded,
                        raftConfig.getLogCompactionCheckIntervalMs(),
                        raftConfig.getLogCompactionCheckIntervalMs(),
                        TimeUnit.MILLISECONDS);
            }

            // Update last activity time
            lastActivityTime.set(System.currentTimeMillis());

            LOGGER.info("Raft node started: term=" + currentTerm.get() +
                      ", role=" + role.get());
        }
    }

    /**
     * Stops this node.
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            LOGGER.info("Stopping Raft node " + nodeId);

            // Stop election timer
            electionTimer.stop();

            // Stop leader state if needed
            LeaderState currentLeaderState = leaderState.get();
            if (currentLeaderState != null) {
                currentLeaderState.stop();
                leaderState.set(null);
            }

            // Stop async applier
            asyncApplier.stop();

            // Stop read index and follower read
            readIndex.stop();
            followerRead.stop();

            // Shutdown executors
            executor.shutdown();
            scheduler.shutdown();

            try {
                executor.awaitTermination(5, TimeUnit.SECONDS);
                scheduler.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.warning("Interrupted while waiting for executors to shutdown");
            }

            LOGGER.info("Raft node stopped");
        }
    }

    /**
     * Sets the leadership change callback.
     *
     * @param callback The callback
     */
    public void setLeadershipChangeCallback(Consumer<Boolean> callback) {
        this.leadershipChangeCallback = callback;
    }

    /**
     * Sets the term change callback.
     *
     * @param callback The callback
     */
    public void setTermChangeCallback(Consumer<Long> callback) {
        this.termChangeCallback = callback;
    }

    /**
     * Sets the role change callback.
     *
     * @param callback The callback
     */
    public void setRoleChangeCallback(Consumer<Role> callback) {
        this.roleChangeCallback = callback;
    }

    /**
     * Handles an AppendEntries request.
     *
     * @param request The request
     * @return The response
     */
    public AppendEntriesResponseMessage handleAppendEntriesRequest(AppendEntriesRequestMessage request) {
        if (!running.get()) {
            return new AppendEntriesResponseMessage(
                    currentTerm.get(), groupId, nodeId, false, -1);
        }

        // Update last activity time
        lastActivityTime.set(System.currentTimeMillis());

        // Reset election timer since we received a valid AppendEntries
        electionTimer.resetTimeout();

        // Update metrics
        metrics.counter("appendentries.request").inc();

        lock.writeLock().lock();
        try {
            long requestTerm = request.getTerm();
            long currentTermValue = currentTerm.get();

            // Check if the request term is smaller than current term
            if (requestTerm < currentTermValue) {
                metrics.counter("appendentries.rejected.term").inc();

                // Reject the request
                return new AppendEntriesResponseMessage(
                        currentTermValue, groupId, nodeId, false, log.getLastIndex());
            }

            // If the request term is greater than current term, update term and convert to follower
            if (requestTerm > currentTermValue) {
                updateTermAndBecomeFollower(requestTerm, null);
            }

            // Update leader ID
            updateLeaderId(request.getLeaderId());

            // Update follower read heartbeat info
            followerRead.updateHeartbeat(requestTerm, request.getLeaderCommit());

            // Check log consistency
            long prevLogIndex = request.getPrevLogIndex();
            long prevLogTerm = request.getPrevLogTerm();

            // For the first entry, prevLogIndex might be 0
            if (prevLogIndex > 0) {
                boolean hasEntry = log.getEntry(prevLogIndex) != null;

                if (!hasEntry || log.getTerm(prevLogIndex) != prevLogTerm) {
                    // Log is inconsistent
                    metrics.counter("appendentries.rejected.inconsistent").inc();

                    // Find the conflict index and term
                    long conflictIndex = -1;
                    long conflictTerm = -1;

                    if (!hasEntry) {
                        // We don't have the entry at prevLogIndex
                        conflictIndex = log.getLastIndex() + 1;
                    } else {
                        // We have an entry at prevLogIndex but with different term
                        conflictTerm = log.getTerm(prevLogIndex);

                        // Find the first index with this term
                        for (long i = prevLogIndex; i > log.getFirstIndex(); i--) {
                            if (log.getTerm(i - 1) != conflictTerm) {
                                conflictIndex = i;
                                break;
                            }
                        }
                    }

                    return new AppendEntriesResponseMessage(
                            currentTerm.get(), groupId, nodeId, false, log.getLastIndex(),
                            conflictIndex, conflictTerm);
                }
            }

            // Log is consistent at prevLogIndex
            metrics.counter("appendentries.consistent").inc();

            // Process entries
            List<LogEntry> entries = request.getEntries();

            if (!entries.isEmpty()) {
                metrics.counter("appendentries.entries").inc(entries.size());

                // Find conflicting entries
                long entryIndex = prevLogIndex;

                for (LogEntry entry : entries) {
                    entryIndex++;

                    LogEntry existingEntry = log.getEntry(entryIndex);
                    if (existingEntry != null && existingEntry.getTerm() != entry.getTerm()) {
                        // Found a conflict, delete all entries from this point on
                        log.deleteEntriesAfter(entryIndex - 1);
                        break;
                    }

                    if (existingEntry == null) {
                        // Reached the end of our log, no need to continue checking
                        break;
                    }
                }

                // Append new entries
                if (entryIndex <= log.getLastIndex()) {
                    // Append remaining entries
                    long firstNewIndex = entryIndex;
                    List<LogEntry> newEntries = new ArrayList<>();

                    for (int i = (int) (firstNewIndex - prevLogIndex - 1); i < entries.size(); i++) {
                        newEntries.add(entries.get(i));
                    }

                    log.appendEntries(newEntries);
                } else {
                    // Append all entries
                    log.appendEntries(entries);
                }
            }

            // Update commit index
            long leaderCommit = request.getLeaderCommit();
            long lastNewIndex = prevLogIndex + entries.size();

            if (leaderCommit > log.getCommitIndex()) {
                log.setCommitIndex(Math.min(leaderCommit, lastNewIndex));
            }

            return new AppendEntriesResponseMessage(
                    currentTerm.get(), groupId, nodeId, true, log.getLastIndex());
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error handling AppendEntries request", e);
            metrics.counter("appendentries.error").inc();

            return new AppendEntriesResponseMessage(
                    currentTerm.get(), groupId, nodeId, false, log.getLastIndex());
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Handles a RequestVote request.
     *
     * @param request The request
     * @return The response
     */
    public VoteResponseMessage handleVoteRequest(VoteRequestMessage request) {
        if (!running.get()) {
            return new VoteResponseMessage(
                    currentTerm.get(), groupId, nodeId, false);
        }

        // Update last activity time
        lastActivityTime.set(System.currentTimeMillis());

        // Update metrics
        metrics.counter("vote.request").inc();

        lock.writeLock().lock();
        try {
            long requestTerm = request.getTerm();
            long currentTermValue = currentTerm.get();

            // Check if the request term is smaller than current term
            if (requestTerm < currentTermValue) {
                metrics.counter("vote.rejected.term").inc();

                // Reject the vote
                return new VoteResponseMessage(
                        currentTermValue, groupId, nodeId, false);
            }

            // If the request term is greater than current term, update term and convert to follower
            if (requestTerm > currentTermValue) {
                updateTermAndBecomeFollower(requestTerm, null);
            }

            // Check if we've already voted for someone else in this term
            String votedForValue = votedFor.get();

            if (votedForValue != null && !votedForValue.equals(request.getNodeId())) {
                metrics.counter("vote.rejected.already_voted").inc();

                // Already voted for someone else
                return new VoteResponseMessage(
                        currentTerm.get(), groupId, nodeId, false);
            }

            // Check if the candidate's log is at least as up-to-date as ours
            long lastLogIndex = log.getLastIndex();
            long lastLogTerm = log.getLastTerm();

            boolean logIsUpToDate = false;

            if (request.getLastLogTerm() > lastLogTerm) {
                // Candidate has a higher term in last log entry
                logIsUpToDate = true;
            } else if (request.getLastLogTerm() == lastLogTerm) {
                // Same term, check index
                logIsUpToDate = request.getLastLogIndex() >= lastLogIndex;
            }

            if (!logIsUpToDate) {
                metrics.counter("vote.rejected.log").inc();

                // Candidate's log is not up-to-date
                return new VoteResponseMessage(
                        currentTerm.get(), groupId, nodeId, false);
            }

            // Grant vote
            metrics.counter("vote.granted").inc();

            // Update voted for
            votedFor.set(request.getNodeId());

            // Persist voted for
            try {
                storage.writeVotedFor(request.getNodeId());
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "Failed to persist voted for", e);
            }

            // Reset election timer
            electionTimer.resetTimeout();

            return new VoteResponseMessage(
                    currentTerm.get(), groupId, nodeId, true);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Handles an InstallSnapshot request.
     *
     * @param request The request
     * @return The response
     */
    public InstallSnapshotResponseMessage handleInstallSnapshotRequest(InstallSnapshotRequestMessage request) {
        if (!running.get()) {
            return new InstallSnapshotResponseMessage(
                    currentTerm.get(), groupId, nodeId, "Node is not running");
        }

        // Update last activity time
        lastActivityTime.set(System.currentTimeMillis());

        // Reset election timer since we received a valid request from the leader
        electionTimer.resetTimeout();

        // Update metrics
        metrics.counter("snapshot.request").inc();

        lock.writeLock().lock();
        try {
            long requestTerm = request.getTerm();
            long currentTermValue = currentTerm.get();

            // Check if the request term is smaller than current term
            if (requestTerm < currentTermValue) {
                metrics.counter("snapshot.rejected.term").inc();

                // Reject the request
                return new InstallSnapshotResponseMessage(
                        currentTermValue, groupId, nodeId, "Term is smaller");
            }

            // If the request term is greater than current term, update term and convert to follower
            if (requestTerm > currentTermValue) {
                updateTermAndBecomeFollower(requestTerm, null);
            }

            // Update leader ID
            updateLeaderId(request.getLeaderId());

            // TODO: Implement snapshot installation
            // For now, just return success

            metrics.counter("snapshot.success").inc();

            return new InstallSnapshotResponseMessage(
                    currentTerm.get(), groupId, nodeId, request.getLastIncludedIndex());
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error handling InstallSnapshot request", e);
            metrics.counter("snapshot.error").inc();

            return new InstallSnapshotResponseMessage(
                    currentTerm.get(), groupId, nodeId, e.getMessage());
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Handles a ReadIndex request.
     *
     * @param request The request
     * @return The response
     */
    public ReadIndexResponseMessage handleReadIndexRequest(ReadIndexRequestMessage request) {
        if (!running.get()) {
            return new ReadIndexResponseMessage(
                    currentTerm.get(), groupId, nodeId, request.getReadId(), "Node is not running");
        }

        // Update last activity time
        lastActivityTime.set(System.currentTimeMillis());

        // Update metrics
        metrics.counter("readindex.request").inc();

        // If we're the leader, process the request
        if (role.get() == Role.LEADER) {
            return readIndex.processReadIndexRequest(request);
        } else {
            // Forward to leader if known
            String currentLeaderId = leaderId.get();
            if (currentLeaderId != null && !currentLeaderId.equals(nodeId)) {
                metrics.counter("readindex.forward").inc();

                // Return response indicating to forward to leader
                return new ReadIndexResponseMessage(
                        currentTerm.get(), groupId, nodeId, request.getReadId(),
                        "Not the leader, forward to " + currentLeaderId);
            } else {
                metrics.counter("readindex.rejected.no_leader").inc();

                // No known leader
                return new ReadIndexResponseMessage(
                        currentTerm.get(), groupId, nodeId, request.getReadId(),
                        "No known leader");
            }
        }
    }

    /**
     * Handles a FollowerRead request.
     *
     * @param request The request
     * @return The response
     */
    public FollowerReadResponseMessage handleFollowerReadRequest(FollowerReadRequestMessage request) {
        if (!running.get()) {
            return new FollowerReadResponseMessage(
                    currentTerm.get(), groupId, nodeId, request.getReadId(), "Node is not running");
        }

        // Update last activity time
        lastActivityTime.set(System.currentTimeMillis());

        // Update metrics
        metrics.counter("followerread.request").inc();

        // If we're the leader, process the request
        if (role.get() == Role.LEADER) {
            return followerRead.processFollowerReadRequest(request);
        } else {
            // Forward to leader if known
            String currentLeaderId = leaderId.get();
            if (currentLeaderId != null && !currentLeaderId.equals(nodeId)) {
                metrics.counter("followerread.forward").inc();

                // Return response indicating to forward to leader
                return new FollowerReadResponseMessage(
                        currentTerm.get(), groupId, nodeId, request.getReadId(),
                        "Not the leader, forward to " + currentLeaderId);
            } else {
                metrics.counter("followerread.rejected.no_leader").inc();

                // No known leader
                return new FollowerReadResponseMessage(
                        currentTerm.get(), groupId, nodeId, request.getReadId(),
                        "No known leader");
            }
        }
    }

    /**
     * Handles an AsyncApplyNotification.
     *
     * @param notification The notification
     */
    private void handleAsyncApplyNotification(AsyncApplyNotificationMessage notification) {
        // Update metrics
        if (notification.isSuccess()) {
            metrics.counter("apply.success").inc();
        } else {
            metrics.counter("apply.failure").inc();
        }

        // If we're the leader, complete the pending request
        if (role.get() == Role.LEADER) {
            LeaderState currentLeaderState = leaderState.get();
            if (currentLeaderState != null) {
                if (notification.isSuccess()) {
                    currentLeaderState.completeRequest(notification.getAppliedIndex(), null);
                } else {
                    currentLeaderState.completeRequestExceptionally(
                            notification.getAppliedIndex(),
                            new RuntimeException(notification.getErrorMessage()));
                }
            }
        }
    }

    /**
     * Starts an election.
     *
     * @return A future that completes with true if elected, false otherwise
     */
    private CompletableFuture<Boolean> startElection() {
        if (!running.get()) {
            return CompletableFuture.completedFuture(false);
        }

        // Update metrics
        metrics.counter("election.start").inc();

        lock.writeLock().lock();
        try {
            // Increment current term
            long newTerm = currentTerm.incrementAndGet();

            // Vote for self
            votedFor.set(nodeId);

            // Transition to candidate state
            role.set(Role.CANDIDATE);

            // Persist term and voted for
            try {
                storage.writeCurrentTerm(newTerm);
                storage.writeVotedFor(nodeId);
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "Failed to persist term and voted for", e);
            }

            // Notify about role change
            if (roleChangeCallback != null) {
                roleChangeCallback.accept(Role.CANDIDATE);
            }

            // Notify about term change
            if (termChangeCallback != null) {
                termChangeCallback.accept(newTerm);
            }

            LOGGER.info("Starting election for term " + newTerm);

            // Reset election timer
            electionTimer.resetTimeout();

            // Get last log info
            long lastLogIndex = log.getLastIndex();
            long lastLogTerm = log.getLastTerm();

            // Create vote request
            VoteRequestMessage request = new VoteRequestMessage(
                    newTerm, groupId, nodeId, lastLogIndex, lastLogTerm);

            // Track votes
            int totalNodes = peers.size() + 1; // including self
            int votesNeeded = (totalNodes / 2) + 1;
            int votesReceived = 1; // vote for self

            // If we're the only node, become leader immediately
            if (votesReceived >= votesNeeded) {
                becomeLeader();
                return CompletableFuture.completedFuture(true);
            }

            // Send vote requests to all peers
            CompletableFuture<Boolean> electionResult = new CompletableFuture<>();
            AtomicLong voteCount = new AtomicLong(1); // Include self
            AtomicBoolean electionDecided = new AtomicBoolean(false);

            peers.forEach((id, peer) -> {
                if (!peer.isVoting()) {
                    // Non-voting peers don't participate in elections
                    return;
                }

                peer.requestVote(request)
                    .thenAccept(response -> {
                        if (electionDecided.get()) {
                            return;
                        }

                        lock.writeLock().lock();
                        try {
                            // Check if we're still a candidate in the same term
                            if (role.get() != Role.CANDIDATE || currentTerm.get() != newTerm) {
                                if (!electionDecided.getAndSet(true)) {
                                    electionResult.complete(false);
                                }
                                return;
                            }

                            // Check if the response has a higher term
                            if (response.getTerm() > currentTerm.get()) {
                                updateTermAndBecomeFollower(response.getTerm(), null);
                                if (!electionDecided.getAndSet(true)) {
                                    electionResult.complete(false);
                                }
                                return;
                            }

                            // Check if the vote was granted
                            if (response.isVoteGranted()) {
                                long newCount = voteCount.incrementAndGet();
                                LOGGER.info("Received vote from " + id +
                                          ", count: " + newCount + "/" + votesNeeded);

                                // Check if we have enough votes
                                if (newCount >= votesNeeded && !electionDecided.getAndSet(true)) {
                                    becomeLeader();
                                    electionResult.complete(true);
                                }
                            }
                        } finally {
                            lock.writeLock().unlock();
                        }
                    })
                    .exceptionally(ex -> {
                        LOGGER.log(Level.WARNING, "Failed to send vote request to " + id, ex);
                        return null;
                    });
            });

            // Set a timeout for the election
            scheduler.schedule(() -> {
                if (!electionDecided.getAndSet(true)) {
                    LOGGER.info("Election for term " + newTerm + " timed out");
                    electionResult.complete(false);
                }
            }, raftConfig.getElectionTimeoutMs(), TimeUnit.MILLISECONDS);

            return electionResult;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Transitions to leader role.
     */
    private void becomeLeader() {
        if (role.get() == Role.LEADER) {
            return;
        }

        lock.writeLock().lock();
        try {
            LOGGER.info("Becoming leader for term " + currentTerm.get());

            // Update role
            role.set(Role.LEADER);

            // Update leader ID
            leaderId.set(nodeId);

            // Stop election timer
            electionTimer.stop();

            // Create leader state
            LeaderState newLeaderState = new LeaderState(
                    currentTerm.get(), nodeId, groupId, log, peers,
                    scheduler, raftConfig.getHeartbeatIntervalMs());

            // Set commit index update callback
            newLeaderState.setCommitIndexUpdateCallback(() -> {
                // Update metrics
                metrics.gauge("commit_index").set(log.getCommitIndex());
            });

            // Update leader state
            leaderState.set(newLeaderState);

            // Start leader state
            newLeaderState.start();

            // Notify about role change
            if (roleChangeCallback != null) {
                roleChangeCallback.accept(Role.LEADER);
            }

            // Notify about leadership change
            if (leadershipChangeCallback != null) {
                leadershipChangeCallback.accept(true);
            }

            // Update metrics
            metrics.counter("leader.become").inc();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Updates the current term and transitions to follower role.
     *
     * @param newTerm The new term
     * @param newLeaderId The new leader ID (may be null)
     */
    private void updateTermAndBecomeFollower(long newTerm, String newLeaderId) {
        lock.writeLock().lock();
        try {
            long oldTerm = currentTerm.get();
            Role oldRole = role.get();

            if (newTerm <= oldTerm) {
                return;
            }

            LOGGER.info("Updating term: " + oldTerm + " -> " + newTerm);

            // Update term
            currentTerm.set(newTerm);

            // Clear voted for
            votedFor.set(null);

            // Persist term and voted for
            try {
                storage.writeCurrentTerm(newTerm);
                storage.writeVotedFor(null);
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "Failed to persist term and voted for", e);
            }

            // If not already a follower, become one
            if (oldRole != Role.FOLLOWER) {
                becomeFollower(newLeaderId);
            } else if (newLeaderId != null) {
                // Just update leader ID
                updateLeaderId(newLeaderId);
            }

            // Notify about term change
            if (termChangeCallback != null) {
                termChangeCallback.accept(newTerm);
            }

            // Update metrics
            metrics.counter("term.update").inc();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Transitions to follower role.
     *
     * @param newLeaderId The new leader ID (may be null)
     */
    private void becomeFollower(String newLeaderId) {
        if (role.get() == Role.FOLLOWER) {
            return;
        }

        lock.writeLock().lock();
        try {
            LOGGER.info("Becoming follower, leader: " + newLeaderId);

            // Update role
            role.set(Role.FOLLOWER);

            // Update leader ID
            updateLeaderId(newLeaderId);

            // Stop leader state if needed
            LeaderState currentLeaderState = leaderState.get();
            if (currentLeaderState != null) {
                currentLeaderState.stop();
                leaderState.set(null);

                // Notify about leadership change
                if (leadershipChangeCallback != null) {
                    leadershipChangeCallback.accept(false);
                }
            }

            // Start election timer
            electionTimer.start();

            // Notify about role change
            if (roleChangeCallback != null) {
                roleChangeCallback.accept(Role.FOLLOWER);
            }

            // Update metrics
            metrics.counter("follower.become").inc();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Updates the leader ID.
     *
     * @param newLeaderId The new leader ID (may be null)
     */
    private void updateLeaderId(String newLeaderId) {
        if (Objects.equals(leaderId.get(), newLeaderId)) {
            return;
        }

        LOGGER.info("Updating leader ID: " + leaderId.get() + " -> " + newLeaderId);
        leaderId.set(newLeaderId);
    }

    /**
     * Appends a new entry to the log.
     *
     * @param command The command to append
     * @return A future that completes with the result of applying the command
     */
    public CompletableFuture<byte[]> appendEntry(byte[] command) {
        if (!running.get()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Node is not running"));
        }

        // Update metrics
        metrics.counter("append.request").inc();

        // Check if we're the leader
        if (role.get() != Role.LEADER) {
            metrics.counter("append.rejected.not_leader").inc();

            // Not the leader, return an error
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Not the leader"));
        }

        // Forward to leader state
        LeaderState currentLeaderState = leaderState.get();
        if (currentLeaderState == null) {
            metrics.counter("append.rejected.no_leader_state").inc();

            // No leader state, return an error
            return CompletableFuture.failedFuture(
                    new IllegalStateException("No leader state"));
        }

        return currentLeaderState.appendEntry(command);
    }

    /**
     * Reads a value using the ReadIndex protocol.
     *
     * @param key The key to read
     * @return A future that completes with the value
     */
    public CompletableFuture<byte[]> readWithReadIndex(byte[] key) {
        if (!running.get()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Node is not running"));
        }

        // Update metrics
        metrics.counter("read.readindex").inc();

        return readIndex.read(key, stateMachine::read);
    }

    /**
     * Reads a value using the FollowerRead protocol.
     *
     * @param key The key to read
     * @return A future that completes with the value
     */
    public CompletableFuture<byte[]> readWithFollowerRead(byte[] key) {
        if (!running.get()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Node is not running"));
        }

        // Update metrics
        metrics.counter("read.followerread").inc();

        return followerRead.read(key, stateMachine::read);
    }

    /**
     * Forwards a ReadIndex request to the leader.
     *
     * @param request The request
     * @return A future that completes with the response
     */
    private CompletableFuture<ReadIndexResponseMessage> forwardReadIndexRequest(ReadIndexRequestMessage request) {
        String currentLeaderId = leaderId.get();
        if (currentLeaderId == null || currentLeaderId.equals(nodeId)) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("No leader to forward to"));
        }

        RaftPeer leaderPeer = peers.get(currentLeaderId);
        if (leaderPeer == null) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Leader peer not found"));
        }

        return leaderPeer.readIndex(request);
    }

    /**
     * Forwards a FollowerRead request to the leader.
     *
     * @param request The request
     * @return A future that completes with the response
     */
    private CompletableFuture<FollowerReadResponseMessage> forwardFollowerReadRequest(FollowerReadRequestMessage request) {
        String currentLeaderId = leaderId.get();
        if (currentLeaderId == null || currentLeaderId.equals(nodeId)) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("No leader to forward to"));
        }

        RaftPeer leaderPeer = peers.get(currentLeaderId);
        if (leaderPeer == null) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Leader peer not found"));
        }

        return leaderPeer.followerRead(request);
    }

    /**
     * Broadcasts heartbeats to all peers.
     *
     * @return A future that completes with true if a majority acknowledged
     */
    public CompletableFuture<Boolean> broadcastHeartbeats() {
        if (role.get() != Role.LEADER) {
            return CompletableFuture.completedFuture(false);
        }

        LeaderState currentLeaderState = leaderState.get();
        if (currentLeaderState == null) {
            return CompletableFuture.completedFuture(false);
        }

        return currentLeaderState.broadcastHeartbeats();
    }

    /**
     * Checks if a snapshot should be taken.
     */
    private void checkSnapshotNeeded() {
        if (!running.get() || role.get() != Role.LEADER) {
            return;
        }

        // TODO: Implement snapshot logic
    }

    /**
     * Checks if log compaction is needed.
     */
    private void checkLogCompactionNeeded() {
        if (!running.get()) {
            return;
        }

        // TODO: Implement log compaction logic
    }

    /**
     * Gets the current role.
     *
     * @return The current role
     */
    public Role getRole() {
        return role.get();
    }

    /**
     * Checks if this node is the leader.
     *
     * @return True if this node is the leader
     */
    public boolean isLeader() {
        return role.get() == Role.LEADER;
    }

    /**
     * Gets the current term.
     *
     * @return The current term
     */
    public long getCurrentTerm() {
        return currentTerm.get();
    }

    /**
     * Gets the ID of the current leader.
     *
     * @return The leader ID, or null if unknown
     */
    public String getLeaderId() {
        return leaderId.get();
    }

    /**
     * Gets the node ID.
     *
     * @return The node ID
     */
    public String getNodeId() {
        return nodeId;
    }

    /**
     * Gets the group ID.
     *
     * @return The group ID
     */
    public String getGroupId() {
        return groupId;
    }

    /**
     * Gets the commit index.
     *
     * @return The commit index
     */
    public long getCommitIndex() {
        return log.getCommitIndex();
    }

    /**
     * Gets the last applied index.
     *
     * @return The last applied index
     */
    public long getLastAppliedIndex() {
        return log.getLastApplied();
    }

    /**
     * Gets the time of the last activity.
     *
     * @return The last activity time
     */
    public long getLastActivityTime() {
        return lastActivityTime.get();
    }

    /**
     * Creates a client session.
     *
     * @return The session ID
     */
    public String createClientSession() {
        String sessionId = UUID.randomUUID().toString();
        clientSessions.add(sessionId);
        return sessionId;
    }

    /**
     * Closes a client session.
     *
     * @param sessionId The session ID
     */
    public void closeClientSession(String sessionId) {
        clientSessions.remove(sessionId);
    }

    /**
     * Gets the node status.
     *
     * @return The node status
     */
    public Status getStatus() {
        Role currentRole = role.get();
        boolean isLeaderNode = currentRole == Role.LEADER;
        String currentLeaderId = leaderId.get();

        return new Status(
                running.get(),
                currentRole.toString(),
                isLeaderNode,
                currentLeaderId,
                currentTerm.get(),
                log.getCommitIndex(),
                log.getLastApplied(),
                log.getLastIndex(),
                System.currentTimeMillis() - lastActivityTime.get());
    }
}