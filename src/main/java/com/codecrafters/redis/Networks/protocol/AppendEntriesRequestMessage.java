package com.distributedkv.network.protocol;

import java.util.List;
import java.util.ArrayList;
import com.distributedkv.raft.LogEntry;

/**
 * Message sent by the leader to replicate log entries and as a heartbeat.
 *
 * This message is used by the leader to:
 * 1. Replicate log entries to followers
 * 2. Send heartbeats (empty AppendEntries messages) to maintain leadership
 * It is a central component of the Raft protocol.
 */
public class AppendEntriesRequestMessage extends RaftMessage {

    /** The ID of the leader */
    private String leaderId;

    /** The index of the log entry immediately preceding new ones */
    private long prevLogIndex;

    /** The term of the log entry at prevLogIndex */
    private long prevLogTerm;

    /** The log entries to store (empty for heartbeat) */
    private List<LogEntry> entries;

    /** The leader's commitIndex */
    private long leaderCommit;

    /**
     * Creates a new append entries request message.
     *
     * @param term The current term of the leader
     * @param groupId The ID of the Raft group
     * @param nodeId The ID of the leader node
     * @param leaderId The ID of the leader
     * @param prevLogIndex The index of the previous log entry
     * @param prevLogTerm The term of the previous log entry
     * @param entries The log entries to append
     * @param leaderCommit The leader's commit index
     */
    public AppendEntriesRequestMessage(long term, String groupId, String nodeId,
                                      String leaderId, long prevLogIndex, long prevLogTerm,
                                      List<LogEntry> entries, long leaderCommit) {
        super(term, groupId, nodeId);
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries != null ? new ArrayList<>(entries) : new ArrayList<>();
        this.leaderCommit = leaderCommit;
    }

    /**
     * Gets the ID of the leader.
     *
     * @return The leader ID
     */
    public String getLeaderId() {
        return leaderId;
    }

    /**
     * Gets the index of the log entry immediately preceding new ones.
     *
     * @return The previous log index
     */
    public long getPrevLogIndex() {
        return prevLogIndex;
    }

    /**
     * Gets the term of the log entry at prevLogIndex.
     *
     * @return The previous log term
     */
    public long getPrevLogTerm() {
        return prevLogTerm;
    }

    /**
     * Gets the log entries to store.
     *
     * @return The log entries to append
     */
    public List<LogEntry> getEntries() {
        return new ArrayList<>(entries);
    }

    /**
     * Gets the leader's commitIndex.
     *
     * @return The leader's commit index
     */
    public long getLeaderCommit() {
        return leaderCommit;
    }

    /**
     * Checks if this is a heartbeat message (no entries).
     *
     * @return True if this is a heartbeat, false otherwise
     */
    public boolean isHeartbeat() {
        return entries.isEmpty();
    }

    @Override
    public String toString() {
        return super.toString().replaceFirst("}$",
                ", leaderId='" + leaderId + '\'' +
                ", prevLogIndex=" + prevLogIndex +
                ", prevLogTerm=" + prevLogTerm +
                ", entries.size=" + entries.size() +
                ", leaderCommit=" + leaderCommit +
                '}');
    }
}