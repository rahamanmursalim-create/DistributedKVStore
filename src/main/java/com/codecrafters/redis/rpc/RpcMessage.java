
package com.codecrafters.redis.rpc;

import java.util.Arrays;
import java.util.Objects;

public final class RpcMessage {

    public enum Type {
        REQUEST,
        RESPONSE,
        VOTE_REQUEST,
        VOTE_RESPONSE,
        APPEND_ENTRIES_REQUEST,
        APPEND_ENTRIES_RESPONSE,
        READ_FRESHNESS_REQUEST,
        READ_FRESHNESS_RESPONSE,
        HEARTBEAT,
        INSTALL_SNAPSHOT,
        ERROR
    }

    private final Type type;
    private final String requestId;
    private final String sourceNodeId;
    private final String targetNodeId;
    private final byte[] payload;

    public RpcMessage(
            Type type,
            String requestId,
            String sourceNodeId,
            String targetNodeId,
            byte[] payload) {

        this.type = Objects.requireNonNull(type, "type");
        this.requestId = requireText(requestId, "requestId");
        this.sourceNodeId = requireText(sourceNodeId, "sourceNodeId");
        this.targetNodeId = requireText(targetNodeId, "targetNodeId");
        this.payload = payload == null ? new byte[0] : payload.clone();
    }

    public Type getType() {
        return type;
    }

    public String getRequestId() {
        return requestId;
    }

    public String getSourceNodeId() {
        return sourceNodeId;
    }

    public String getTargetNodeId() {
        return targetNodeId;
    }

    public byte[] getPayload() {
        return payload.clone();
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    field + " cannot be blank");
        }
        return value;
    }

    @Override
    public String toString() {
        return "RpcMessage{" +
                "type=" + type +
                ", requestId='" + requestId + '\'' +
                ", sourceNodeId='" + sourceNodeId + '\'' +
                ", targetNodeId='" + targetNodeId + '\'' +
                ", payloadBytes=" + payload.length +
                '}';
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (!(object instanceof RpcMessage other)) return false;

        return type == other.type
                && requestId.equals(other.requestId)
                && sourceNodeId.equals(other.sourceNodeId)
                && targetNodeId.equals(other.targetNodeId)
                && Arrays.equals(payload, other.payload);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(
                type, requestId, sourceNodeId, targetNodeId);
        return 31 * result + Arrays.hashCode(payload);
    }
}
