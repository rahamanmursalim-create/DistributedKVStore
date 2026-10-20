package com.distributedkv.network.protocol;

/**
 * Message sent in response to an install snapshot request.
 *
 * This message is sent by followers to inform the leader about the result of
 * an InstallSnapshotRequestMessage. It provides feedback to the leader about
 * the snapshot installation process.
 */
public class InstallSnapshotResponseMessage extends RaftMessage {

    /** The last index written to the follower's state */
    private long lastIndex;

    /** Indicates whether the snapshot chunk was successfully processed */
    private boolean success;

    /** Indicates the reason for failure if success is false */
    private String errorMessage;

    /**
     * Creates a new install snapshot response message indicating success.
     *
     * @param term The current term of the follower
     * @param groupId The ID of the Raft group
     * @param nodeId The ID of the follower node
     * @param lastIndex The last index written to the follower's state
     */
    public InstallSnapshotResponseMessage(long term, String groupId, String nodeId,
                                         long lastIndex) {
        super(term, groupId, nodeId);
        this.lastIndex = lastIndex;
        this.success = true;
        this.errorMessage = null;
    }

    /**
     * Creates a new install snapshot response message with error information.
     *
     * @param term The current term of the follower
     * @param groupId The ID of the Raft group
     * @param nodeId The ID of the follower node
     * @param errorMessage The reason for failure
     */
    public InstallSnapshotResponseMessage(long term, String groupId, String nodeId,
                                         String errorMessage) {
        super(term, groupId, nodeId);
        this.lastIndex = -1;
        this.success = false;
        this.errorMessage = errorMessage;
    }

    /**
     * Gets the last index written to the follower's state.
     *
     * @return The last index
     */
    public long getLastIndex() {
        return lastIndex;
    }

    /**
     * Checks if the snapshot chunk was successfully processed.
     *
     * @return True if successful, false otherwise
     */
    public boolean isSuccess() {
        return success;
    }

    /**
     * Gets the error message if the operation failed.
     *
     * @return The error message
     */
    public String getErrorMessage() {
        return errorMessage;
    }

    @Override
    public String toString() {
        return super.toString().replaceFirst("}$",
                ", success=" + success +
                (success ? ", lastIndex=" + lastIndex :
                          ", errorMessage='" + errorMessage + '\'') +
                '}');
    }
}