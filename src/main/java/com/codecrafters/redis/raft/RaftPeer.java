package com.distributedkv.raft;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import com.distributedkv.network.client.RpcClient;
import com.distributedkv.network.protocol.AppendEntriesRequestMessage;
import com.distributedkv.network.protocol.AppendEntriesResponseMessage;
import com.distributedkv.network.protocol.InstallSnapshotRequestMessage;
import com.distributedkv.network.protocol.InstallSnapshotResponseMessage;
import com.distributedkv.network.protocol.Message;
import com.distributedkv.network.protocol.VoteRequestMessage;
import com.distributedkv.network.protocol.VoteResponseMessage;
import com.distributedkv.network.protocol.ReadIndexRequestMessage;
import com.distributedkv.network.protocol.ReadIndexResponseMessage;
import com.distributedkv.network.protocol.FollowerReadRequestMessage;
import com.distributedkv.network.protocol.FollowerReadResponseMessage;

/**
 * Represents a peer node in a Raft group.
 *
 * RaftPeer encapsulates the state and communication channel to a peer node
 * in the Raft consensus protocol. It provides methods for sending RPC messages
 * to the peer and tracking replication state.
 */
public class RaftPeer {
    private static final Logger LOGGER = Logger.getLogger(RaftPeer.class.getName());

    /** The ID of this peer */
    private final String id;

    /** The endpoint (host:port) of this peer */
    private final String endpoint;

    /** The next log index to send to this peer */
    private final AtomicLong nextIndex;

    /** The highest log index known to be replicated on this peer */
    private final AtomicLong matchIndex;

    /** The RPC client for communicating with this peer */
    private final RpcClient rpcClient;

    /** The group ID of the Raft group this peer belongs to */
    private final String groupId;

    /** Flag indicating whether this peer is voting */
    private boolean voting = true;

    /** The time of the last successful response from this peer */
    private volatile long lastResponseTime;

    /** The time of the last append entries request sent to this peer */
    private volatile long lastAppendTime;

    /** Status of the last append entries request */
    private volatile boolean lastAppendSucceeded = false;

    /**
     * Creates a new Raft peer.
     *
     * @param id The ID of this peer
     * @param endpoint The endpoint of this peer
     * @param lastLogIndex The index of the last log entry
     * @param rpcClient The RPC client
     * @param groupId The Raft group ID
     */
    public RaftPeer(String id, String endpoint, long lastLogIndex, RpcClient rpcClient, String groupId) {
        this.id = id;
        this.endpoint = endpoint;
        this.nextIndex = new AtomicLong(lastLogIndex + 1);
        this.matchIndex = new AtomicLong(0);
        this.rpcClient = rpcClient;
        this.groupId = groupId;
        this.lastResponseTime = System.currentTimeMillis();
        this.lastAppendTime = 0;
    }

    /**
     * Gets the ID of this peer.
     *
     * @return The peer ID
     */
    public String getId() {
        return id;
    }

    /**
     * Gets the endpoint of this peer.
     *
     * @return The endpoint
     */
    public String getEndpoint() {
        return endpoint;
    }

    /**
     * Gets the next log index to send to this peer.
     *
     * @return The next index
     */
    public long getNextIndex() {
        return nextIndex.get();
    }

    /**
     * Sets the next log index to send to this peer.
     *
     * @param nextIndex The next index
     */
    public void setNextIndex(long nextIndex) {
        this.nextIndex.set(nextIndex);
    }

    /**
     * Decrements the next index.
     *
     * @return The new next index
     */
    public long decrementNextIndex() {
        return nextIndex.decrementAndGet();
    }

    /**
     * Sets the next index based on the conflict information in an AppendEntries response.
     *
     * @param response The response
     * @return The new next index
     */
    public long optimizeNextIndex(AppendEntriesResponseMessage response) {
        if (response.hasConflict()) {
            long conflictTerm = response.getConflictTerm();
            long conflictIndex = response.getConflictIndex();

            if (conflictTerm > 0) {
                // The follower has an entry at conflictIndex with term conflictTerm
                // Set nextIndex to the last entry of conflictTerm
                return nextIndex.updateAndGet(current -> Math.min(current, conflictIndex));
            } else {
                // The follower doesn't have an entry at prevLogIndex
                // Set nextIndex to conflictIndex
                return nextIndex.updateAndGet(current -> Math.min(current, conflictIndex));
            }
        } else {
            // If this is a success response, nextIndex should already be updated
            // by the replicate method
            return nextIndex.get();
        }
    }

    /**
     * Gets the highest log index known to be replicated on this peer.
     *
     * @return The match index
     */
    public long getMatchIndex() {
        return matchIndex.get();
    }

    /**
     * Updates the match index if the new value is higher.
     *
     * @param matchIndex The new match index
     * @return True if the match index was updated
     */
    public boolean updateMatchIndex(long matchIndex) {
        long current;
        do {
            current = this.matchIndex.get();
            if (matchIndex <= current) {
                return false;
            }
        } while (!this.matchIndex.compareAndSet(current, matchIndex));

        return true;
    }

    /**
     * Checks if this peer is voting.
     *
     * @return True if this peer is voting
     */
    public boolean isVoting() {
        return voting;
    }

    /**
     * Sets whether this peer is voting.
     *
     * @param voting Whether this peer is voting
     */
    public void setVoting(boolean voting) {
        this.voting = voting;
    }

    /**
     * Sends an AppendEntries RPC to this peer.
     *
     * @param request The request to send
     * @return A future that will complete with the response
     */
    public CompletableFuture<AppendEntriesResponseMessage> appendEntries(AppendEntriesRequestMessage request) {
        lastAppendTime = System.currentTimeMillis();

        return sendRpc(request)
                .thenApply(response -> (AppendEntriesResponseMessage) response)
                .thenApply(response -> {
                    lastResponseTime = System.currentTimeMillis();
                    lastAppendSucceeded = response.isSuccess();

                    if (response.isSuccess()) {
                        // Update indices
                        long lastEntryIndex = request.getPrevLogIndex() + request.getEntries().size();
                        nextIndex.updateAndGet(current -> Math.max(current, lastEntryIndex + 1));
                        updateMatchIndex(lastEntryIndex);
                    } else {
                        // Optimize nextIndex for the next attempt
                        optimizeNextIndex(response);
                    }

                    return response;
                });
    }

    /**
     * Sends a RequestVote RPC to this peer.
     *
     * @param request The request to send
     * @return A future that will complete with the response
     */
    public CompletableFuture<VoteResponseMessage> requestVote(VoteRequestMessage request) {
        return sendRpc(request)
                .thenApply(response -> {
                    lastResponseTime = System.currentTimeMillis();
                    return (VoteResponseMessage) response;
                });
    }

    /**
     * Sends an InstallSnapshot RPC to this peer.
     *
     * @param request The request to send
     * @return A future that will complete with the response
     */
    public CompletableFuture<InstallSnapshotResponseMessage> installSnapshot(InstallSnapshotRequestMessage request) {
        return sendRpc(request)
                .thenApply(response -> {
                    lastResponseTime = System.currentTimeMillis();
                    return (InstallSnapshotResponseMessage) response;
                })
                .thenApply(response -> {
                    if (response.isSuccess()) {
                        // Update indices
                        long snapshotIndex = request.getLastIncludedIndex();
                        nextIndex.updateAndGet(current -> Math.max(current, snapshotIndex + 1));
                        updateMatchIndex(snapshotIndex);
                    }

                    return response;
                });
    }

    /**
     * Sends a ReadIndex RPC to this peer.
     *
     * @param request The request to send
     * @return A future that will complete with the response
     */
    public CompletableFuture<ReadIndexResponseMessage> readIndex(ReadIndexRequestMessage request) {
        return sendRpc(request)
                .thenApply(response -> {
                    lastResponseTime = System.currentTimeMillis();
                    return (ReadIndexResponseMessage) response;
                });
    }

    /**
     * Sends a FollowerRead RPC to this peer.
     *
     * @param request The request to send
     * @return A future that will complete with the response
     */
    public CompletableFuture<FollowerReadResponseMessage> followerRead(FollowerReadRequestMessage request) {
        return sendRpc(request)
                .thenApply(response -> {
                    lastResponseTime = System.currentTimeMillis();
                    return (FollowerReadResponseMessage) response;
                });
    }

    /**
     * Sends an RPC to this peer.
     *
     * @param request The request to send
     * @return A future that will complete with the response
     */
    private CompletableFuture<Message> sendRpc(Message request) {
        return rpcClient.sendRequest(endpoint, request);
    }

    /**
     * Gets the time of the last successful response from this peer.
     *
     * @return The last response time
     */
    public long getLastResponseTime() {
        return lastResponseTime;
    }

    /**
     * Gets the time of the last append entries request sent to this peer.
     *
     * @return The last append time
     */
    public long getLastAppendTime() {
        return lastAppendTime;
    }

    /**
     * Gets the status of the last append entries request.
     *
     * @return True if the last append succeeded
     */
    public boolean isLastAppendSucceeded() {
        return lastAppendSucceeded;
    }

    /**
     * Checks if this peer is healthy.
     *
     * @param maxSilentPeriodMs The maximum allowed silent period
     * @return True if this peer is healthy
     */
    public boolean isHealthy(long maxSilentPeriodMs) {
        return System.currentTimeMillis() - lastResponseTime <= maxSilentPeriodMs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RaftPeer raftPeer = (RaftPeer) o;
        return Objects.equals(id, raftPeer.id) &&
                Objects.equals(groupId, raftPeer.groupId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, groupId);
    }

    @Override
    public String toString() {
        return "RaftPeer{" +
                "id='" + id + '\'' +
                ", endpoint='" + endpoint + '\'' +
                ", nextIndex=" + nextIndex.get() +
                ", matchIndex=" + matchIndex.get() +
                ", voting=" + voting +
                ", lastAppendSucceeded=" + lastAppendSucceeded +
                '}';
    }
}