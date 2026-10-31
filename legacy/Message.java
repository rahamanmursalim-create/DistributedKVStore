package com.distributedkv.network.protocol;

import java.io.Serializable;
import java.util.UUID;

/**
 * Base class for all network messages in the distributed KV system
 */
public abstract class Message implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Message types
     */
    public enum Type {
        // Request types
        REQUEST_PING,
        REQUEST_KV_GET,
        REQUEST_KV_PUT,
        REQUEST_KV_DELETE,
        REQUEST_KV_SCAN,
        REQUEST_KV_BATCH,
        REQUEST_RAFT_VOTE,
        REQUEST_RAFT_APPEND_ENTRIES,
        REQUEST_RAFT_INSTALL_SNAPSHOT,
        REQUEST_RAFT_TRANSFER_LEADERSHIP,
        REQUEST_RAFT_READ_INDEX,
        REQUEST_RAFT_HEARTBEAT,
        REQUEST_MEMBERSHIP_JOIN,
        REQUEST_MEMBERSHIP_LEAVE,
        REQUEST_MEMBERSHIP_STATUS,
        REQUEST_SHARD_INFO,
        REQUEST_SHARD_MIGRATE,

        // Response types
        RESPONSE_SUCCESS,
        RESPONSE_ERROR,
        RESPONSE_KV_GET,
        RESPONSE_KV_SCAN,
        RESPONSE_RAFT_VOTE,
        RESPONSE_RAFT_APPEND_ENTRIES,
        RESPONSE_RAFT_INSTALL_SNAPSHOT,
        RESPONSE_RAFT_READ_INDEX,
        RESPONSE_MEMBERSHIP_STATUS,
        RESPONSE_SHARD_INFO
    }

    // Message identifier
    private final String id;

    // The message type
    private final Type type;

    // Timestamp when the message was created
    private final long timestamp;

    // Source node identifier
    private final String sourceNodeId;

    // Destination node identifier (can be null for broadcast)
    private final String destNodeId;

    // The group identifier this message belongs to
    private final String groupId;

    /**
     * Create a new message
     *
     * @param type the message type
     * @param sourceNodeId the source node identifier
     * @param destNodeId the destination node identifier
     * @param groupId the group identifier
     */
    protected Message(Type type, String sourceNodeId, String destNodeId, String groupId) {
        this.id = UUID.randomUUID().toString();
        this.type = type;
        this.timestamp = System.currentTimeMillis();
        this.sourceNodeId = sourceNodeId;
        this.destNodeId = destNodeId;
        this.groupId = groupId;
    }

    /**
     * Create a new message with a specified ID
     *
     * @param id the message identifier
     * @param type the message type
     * @param sourceNodeId the source node identifier
     * @param destNodeId the destination node identifier
     * @param groupId the group identifier
     */
    protected Message(String id, Type type, String sourceNodeId, String destNodeId, String groupId) {
        this.id = id;
        this.type = type;
        this.timestamp = System.currentTimeMillis();
        this.sourceNodeId = sourceNodeId;
        this.destNodeId = destNodeId;
        this.groupId = groupId;
    }

    /**
     * Get the message identifier
     *
     * @return the message ID
     */
    public String getId() {
        return id;
    }

    /**
     * Get the message type
     *
     * @return the message type
     */
    public Type getType() {
        return type;
    }

    /**
     * Get the message timestamp
     *
     * @return the timestamp when the message was created
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Get the source node identifier
     *
     * @return the source node ID
     */
    public String getSourceNodeId() {
        return sourceNodeId;
    }

    /**
     * Get the destination node identifier
     *
     * @return the destination node ID
     */
    public String getDestNodeId() {
        return destNodeId;
    }

    /**
     * Get the group identifier
     *
     * @return the group ID
     */
    public String getGroupId() {
        return groupId;
    }

    /**
     * Check if this is a request message
     *
     * @return true if this is a request message
     */
    public boolean isRequest() {
        return type.name().startsWith("REQUEST_");
    }

    /**
     * Check if this is a response message
     *
     * @return true if this is a response message
     */
    public boolean isResponse() {
        return type.name().startsWith("RESPONSE_");
    }

    /**
     * Check if this is a Raft message
     *
     * @return true if this is a Raft message
     */
    public boolean isRaftMessage() {
        return type.name().contains("RAFT_");
    }

    /**
     * Check if this is a Key-Value message
     *
     * @return true if this is a KV message
     */
    public boolean isKVMessage() {
        return type.name().contains("KV_");
    }

    /**
     * Check if this is a membership message
     *
     * @return true if this is a membership message
     */
    public boolean isMembershipMessage() {
        return type.name().contains("MEMBERSHIP_");
    }

    /**
     * Check if this is a shard message
     *
     * @return true if this is a shard message
     */
    public boolean isShardMessage() {
        return type.name().contains("SHARD_");
    }

    @Override
    public String toString() {
        return "Message{" +
                "id='" + id + '\'' +
                ", type=" + type +
                ", timestamp=" + timestamp +
                ", sourceNodeId='" + sourceNodeId + '\'' +
                ", destNodeId='" + destNodeId + '\'' +
                ", groupId='" + groupId + '\'' +
                '}';
    }
}