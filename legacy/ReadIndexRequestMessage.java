package com.distributedkv.network.protocol;

/**
 * Message sent by a node to request a read index from the leader.
 *
 * This message is part of the ReadIndex optimization in Raft, which allows
 * followers to serve read requests without forwarding them to the leader,
 * while still maintaining linearizable semantics.
 */
public class ReadIndexRequestMessage extends RaftMessage {

    /** The read request ID */
    private String readId;

    /** The key to read (optional, can be null for non-key-specific reads) */
    private byte[] key;

    /**
     * Creates a new read index request message.
     *
     * @param term The current term of the sender
     * @param groupId The ID of the Raft group
     * @param nodeId The ID of the sender node
     * @param readId The read request ID
     */
    public ReadIndexRequestMessage(long term, String groupId, String nodeId, String readId) {
        this(term, groupId, nodeId, readId, null);
    }

    /**
     * Creates a new read index request message with a specific key.
     *
     * @param term The current term of the sender
     * @param groupId The ID of the Raft group
     * @param nodeId The ID of the sender node
     * @param readId The read request ID
     * @param key The key to read
     */
    public ReadIndexRequestMessage(long term, String groupId, String nodeId,
                                 String readId, byte[] key) {
        super(term, groupId, nodeId);
        this.readId = readId;
        this.key = key;
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
     * Gets the key to read.
     *
     * @return The key
     */
    public byte[] getKey() {
        return key;
    }

    /**
     * Checks if a specific key is requested.
     *
     * @return True if a key is specified, false otherwise
     */
    public boolean hasKey() {
        return key != null && key.length > 0;
    }

    @Override
    public String toString() {
        return super.toString().replaceFirst("}$",
                ", readId='" + readId + '\'' +
                ", hasKey=" + hasKey() +
                '}');
    }
}