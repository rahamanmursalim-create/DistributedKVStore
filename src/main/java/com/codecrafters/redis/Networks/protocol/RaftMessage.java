package com.distributedkv.network.protocol;

import java.util.List;
import java.util.UUID;

/**
 * Base class for all Raft protocol messages.
 *
 * RaftMessage provides the common structure and properties needed for Raft consensus
 * protocol communication. This includes message identification, term information,
 * and other metadata required for the Raft algorithm.
 */
public abstract class RaftMessage extends Message {

    /** The current term of the sender */
    private long term;

    /** The ID of the Raft group this message belongs to */
    private String groupId;

    /** The ID of the node that sent this message */
    private String nodeId;

    /** A unique identifier for this message */
    private final String messageId;

    /**
     * Creates a new RaftMessage.
     *
     * @param term The current term of the sender
     * @param groupId The ID of the Raft group
     * @param nodeId The ID of the sending node
     */
    protected RaftMessage(long term, String groupId, String nodeId) {
        this.term = term;
        this.groupId = groupId;
        this.nodeId = nodeId;
        this.messageId = UUID.randomUUID().toString();
    }

    /**
     * Gets the current term of the sender.
     *
     * @return The current term
     */
    public long getTerm() {
        return term;
    }

    /**
     * Sets the current term of the sender.
     *
     * @param term The current term
     */
    public void setTerm(long term) {
        this.term = term;
    }

    /**
     * Gets the ID of the Raft group this message belongs to.
     *
     * @return The Raft group ID
     */
    public String getGroupId() {
        return groupId;
    }

    /**
     * Sets the ID of the Raft group this message belongs to.
     *
     * @param groupId The Raft group ID
     */
    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    /**
     * Gets the ID of the node that sent this message.
     *
     * @return The node ID
     */
    public String getNodeId() {
        return nodeId;
    }

    /**
     * Sets the ID of the node that sent this message.
     *
     * @param nodeId The node ID
     */
    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * Gets the unique identifier for this message.
     *
     * @return The message ID
     */
    public String getMessageId() {
        return messageId;
    }

    /**
     * Returns a string representation of this message.
     *
     * @return A string representation of this message
     */
    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                "term=" + term +
                ", groupId='" + groupId + '\'' +
                ", nodeId='" + nodeId + '\'' +
                ", messageId='" + messageId + '\'' +
                '}';
    }
}