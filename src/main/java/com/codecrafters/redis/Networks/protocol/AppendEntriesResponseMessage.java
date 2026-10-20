package com.distributedkv.network.protocol;

/**
 * Message sent in response to an append entries request.
 *
 * This message is sent by followers to inform the leader about the result of
 * an AppendEntriesRequestMessage. It indicates whether the operation succeeded
 * and provides information to help the leader update its state.
 */
public class AppendEntriesResponseMessage extends RaftMessage {

    /** Indicates whether the append operation was successful */
    private boolean success;

    /** The index of the last entry in the follower's log */
    private long lastLogIndex;

    /** If the operation failed due to log inconsistency, this helps the leader retransmit */
    private long conflictIndex;

    /** The term of the conflicting entry (if any) */
    private long conflictTerm;

    /**
     * Creates a new append entries response message.
     *
     * @param term The current term of the follower
     * @param groupId The ID of the Raft group
     * @param nodeId The ID of the follower node
     * @param success Whether the append operation was successful
     * @param lastLogIndex The index of the last entry in the follower's log
     */
    public AppendEntriesResponseMessage(long term, String groupId, String nodeId,
                                       boolean success, long lastLogIndex) {
        this(term, groupId, nodeId, success, lastLogIndex, -1, -1);
    }

    /**
     * Creates a new append entries response message with conflict information.
     *
     * @param term The current term of the follower
     * @param groupId The ID of the Raft group
     * @param nodeId The ID of the follower node
     * @param success Whether the append operation was successful
     * @param lastLogIndex The index of the last entry in the follower's log
     * @param conflictIndex The index of the conflicting entry
     * @param conflictTerm The term of the conflicting entry
     */
    public AppendEntriesResponseMessage(long term, String groupId, String nodeId,
                                       boolean success, long lastLogIndex,
                                       long conflictIndex, long conflictTerm) {
        super(term, groupId, nodeId);
        this.success = success;
        this.lastLogIndex = lastLogIndex;
        this.conflictIndex = conflictIndex;
        this.conflictTerm = conflictTerm;
    }

    /**
     * Checks if the append operation was successful.
     *
     * @return True if the operation was successful, false otherwise
     */
    public boolean isSuccess() {
        return success;
    }

    /**
     * Gets the index of the last entry in the follower's log.
     *
     * @return The last log index
     */
    public long getLastLogIndex() {
        return lastLogIndex;
    }

    /**
     * Gets the index of the conflicting entry.
     *
     * @return The conflict index
     */
    public long getConflictIndex() {
        return conflictIndex;
    }

    /**
     * Gets the term of the conflicting entry.
     *
     * @return The conflict term
     */
    public long getConflictTerm() {
        return conflictTerm;
    }

    /**
     * Checks if conflict information is available.
     *
     * @return True if conflict information is available, false otherwise
     */
    public boolean hasConflict() {
        return conflictIndex >= 0 && conflictTerm >= 0;
    }

    @Override
    public String toString() {
        return super.toString().replaceFirst("}$",
                ", success=" + success +
                ", lastLogIndex=" + lastLogIndex +
                (hasConflict() ? ", conflictIndex=" + conflictIndex +
                                ", conflictTerm=" + conflictTerm : "") +
                '}');
    }
}