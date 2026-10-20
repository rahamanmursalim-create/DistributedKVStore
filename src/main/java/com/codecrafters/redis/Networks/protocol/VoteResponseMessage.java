package com.distributedkv.network.protocol;

/**
 * Message sent in response to a vote request.
 *
 * This message is sent by nodes in response to a VoteRequestMessage.
 * It indicates whether the node granted its vote to the candidate.
 */
public class VoteResponseMessage extends RaftMessage {

    /** Indicates whether the vote was granted */
    private boolean voteGranted;

    /**
     * Creates a new vote response message.
     *
     * @param term The current term of the responding node
     * @param groupId The ID of the Raft group
     * @param nodeId The ID of the responding node
     * @param voteGranted Whether the vote was granted
     */
    public VoteResponseMessage(long term, String groupId, String nodeId, boolean voteGranted) {
        super(term, groupId, nodeId);
        this.voteGranted = voteGranted;
    }

    /**
     * Checks if the vote was granted.
     *
     * @return True if the vote was granted, false otherwise
     */
    public boolean isVoteGranted() {
        return voteGranted;
    }

    /**
     * Sets whether the vote was granted.
     *
     * @param voteGranted Whether the vote was granted
     */
    public void setVoteGranted(boolean voteGranted) {
        this.voteGranted = voteGranted;
    }

    @Override
    public String toString() {
        return super.toString().replaceFirst("}$",
                ", voteGranted=" + voteGranted +
                '}');
    }
}