package com.distributedkv.network.protocol;

/**
 * Message sent by the leader to transfer a snapshot to a follower.
 *
 * This message is part of the log compaction mechanism in Raft. When a follower
 * is far behind the leader or after a new node joins the cluster, the leader
 * sends a snapshot of its state to bring the follower up to date.
 */
public class InstallSnapshotRequestMessage extends RaftMessage {

    /** The ID of the leader */
    private String leaderId;

    /** The last included index in the snapshot */
    private long lastIncludedIndex;

    /** The term of the last included entry */
    private long lastIncludedTerm;

    /** The offset of the data chunk in the snapshot file */
    private long offset;

    /** The snapshot data */
    private byte[] data;

    /** Flag indicating if this is the last chunk */
    private boolean done;

    /** Snapshot metadata (optional) */
    private byte[] metadata;

    /**
     * Creates a new install snapshot request message.
     *
     * @param term The current term of the leader
     * @param groupId The ID of the Raft group
     * @param nodeId The ID of the leader node
     * @param leaderId The ID of the leader
     * @param lastIncludedIndex The last included index in the snapshot
     * @param lastIncludedTerm The term of the last included entry
     * @param offset The offset of the data chunk
     * @param data The snapshot data
     * @param done Flag indicating if this is the last chunk
     */
    public InstallSnapshotRequestMessage(long term, String groupId, String nodeId,
                                        String leaderId, long lastIncludedIndex,
                                        long lastIncludedTerm, long offset,
                                        byte[] data, boolean done) {
        this(term, groupId, nodeId, leaderId, lastIncludedIndex, lastIncludedTerm,
             offset, data, done, null);
    }

    /**
     * Creates a new install snapshot request message with metadata.
     *
     * @param term The current term of the leader
     * @param groupId The ID of the Raft group
     * @param nodeId The ID of the leader node
     * @param leaderId The ID of the leader
     * @param lastIncludedIndex The last included index in the snapshot
     * @param lastIncludedTerm The term of the last included entry
     * @param offset The offset of the data chunk
     * @param data The snapshot data
     * @param done Flag indicating if this is the last chunk
     * @param metadata The snapshot metadata
     */
    public InstallSnapshotRequestMessage(long term, String groupId, String nodeId,
                                        String leaderId, long lastIncludedIndex,
                                        long lastIncludedTerm, long offset,
                                        byte[] data, boolean done, byte[] metadata) {
        super(term, groupId, nodeId);
        this.leaderId = leaderId;
        this.lastIncludedIndex = lastIncludedIndex;
        this.lastIncludedTerm = lastIncludedTerm;
        this.offset = offset;
        this.data = data;
        this.done = done;
        this.metadata = metadata;
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
     * Gets the last included index in the snapshot.
     *
     * @return The last included index
     */
    public long getLastIncludedIndex() {
        return lastIncludedIndex;
    }

    /**
     * Gets the term of the last included entry.
     *
     * @return The last included term
     */
    public long getLastIncludedTerm() {
        return lastIncludedTerm;
    }

    /**
     * Gets the offset of the data chunk.
     *
     * @return The offset
     */
    public long getOffset() {
        return offset;
    }

    /**
     * Gets the snapshot data.
     *
     * @return The data
     */
    public byte[] getData() {
        return data;
    }

    /**
     * Checks if this is the last chunk.
     *
     * @return True if this is the last chunk, false otherwise
     */
    public boolean isDone() {
        return done;
    }

    /**
     * Gets the snapshot metadata.
     *
     * @return The metadata
     */
    public byte[] getMetadata() {
        return metadata;
    }

    /**
     * Checks if metadata is available.
     *
     * @return True if metadata is available, false otherwise
     */
    public boolean hasMetadata() {
        return metadata != null && metadata.length > 0;
    }

    @Override
    public String toString() {
        return super.toString().replaceFirst("}$",
                ", leaderId='" + leaderId + '\'' +
                ", lastIncludedIndex=" + lastIncludedIndex +
                ", lastIncludedTerm=" + lastIncludedTerm +
                ", offset=" + offset +
                ", dataSize=" + (data != null ? data.length : 0) +
                ", done=" + done +
                ", hasMetadata=" + hasMetadata() +
                '}');
    }
}