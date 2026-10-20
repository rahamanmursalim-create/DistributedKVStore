package com.distributedkv.network.protocol;

/**
 * Message sent by candidates to request votes from other nodes.
 *
 * This message is part of the leader election process in the Raft consensus protocol.
 * Candidates send this message to all other nodes in the Raft group to request their vote.
 */
public class VoteRequestMessage extends RaftMessage {

    /** The index of the candidate's last log entry */
    private long lastLogIndex;

    /** The term of the candidate's last log entry */
    private long lastLogTerm;

    /**
     * Creates a new vote request message.
     *
     * @param term The current term of the candidate
     * @param groupId The ID of the Raft group
     * @param nodeId The ID of the candidate node
     * @param lastLogIndex The index of the candidate's last log entry
     * @param lastLogTerm The term of the candidate's last log entry
     */
    public VoteRequestMessage(long term, String groupId, String nodeId,
                              long lastLogIndex, long lastLogTerm) {
        super(term, groupId, nodeId);
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    /**
     * Gets the index of the candidate's last log entry.
     *
     * @return The last log index
     */
    public long getLastLogIndex() {
        return lastLogIndex;
    }

    /**
     * Gets the term of the candidate's last log entry.
     *
     * @return The last log term
     */
    public long getLastLogTerm() {
        return lastLogTerm;
    }

    @Override
    public String toString() {
        return super.toString().replaceFirst("}$",
                ", lastLogIndex=" + lastLogIndex +
                ", lastLogTerm=" + lastLogTerm +
                '}');
    }
}