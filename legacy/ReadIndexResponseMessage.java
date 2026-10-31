package com.distributedkv.network.protocol;

/**
 * Message sent by the leader in response to a read index request.
 *
 * This message provides the commit index needed by the follower to
 * serve read requests with linearizable semantics. It is part of the
 * ReadIndex optimization in Raft.
 */
public class ReadIndexResponseMessage extends RaftMessage {

    /** The read request ID (must match the ID in the request) */
    private String readId;

    /** The index that the follower must wait to be applied before serving the read */
    private long readIndex;

    /** Whether this response indicates success */
    private boolean success;

    /** Error message if not successful */
    private String errorMessage;

    /**
     * Creates a new successful read index response message.
     *
     * @param term The current term of the leader
     * @param groupId The ID of the Raft group
     * @param nodeId The ID of the leader node
     * @param readId The read request ID
     * @param readIndex The read index
     */
    public ReadIndexResponseMessage(long term, String groupId, String nodeId,
                                   String readId, long readIndex) {
        super(term, groupId, nodeId);
        this.readId = readId;
        this.readIndex = readIndex;
        this.success = true;
        this.errorMessage = null;
    }

    /**
     * Creates a new unsuccessful read index response message.
     *
     * @param term The current term of the leader
     * @param groupId The ID of the Raft group
     * @param nodeId The ID of the leader node
     * @param readId The read request ID
     * @param errorMessage The error message
     */
    public ReadIndexResponseMessage(long term, String groupId, String nodeId,
                                   String readId, String errorMessage) {
        super(term, groupId, nodeId);
        this.readId = readId;
        this.readIndex = -1;
        this.success = false;
        this.errorMessage = errorMessage;
    }

    /**
     * Gets the read request ID.
     *
     * @return The read ID
     */
    public String getReadId() {
        return readId;
    }

    /**
     * Gets the read index.
     *
     * @return The read index
     */
    public long getReadIndex() {
        return readIndex;
    }

    /**
     * Checks if the response indicates success.
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
                ", readId='" + readId + '\'' +
                (success ? ", readIndex=" + readIndex :
                          ", errorMessage='" + errorMessage + '\'') +
                ", success=" + success +
                '}');
    }
}