package com.codecrafters.redis.distributed;

import java.util.Objects;

/**
 * Represents one node in the distributed key-value store.
 *
 * This class stores node metadata only. It does not open a network
 * connection or replicate data.
 */
public final class StorageNode {

    private final String nodeId;
    private final String host;
    private final int port;

    private volatile boolean active;

    public StorageNode(String nodeId, String host, int port) {
        this.nodeId = requireText(nodeId, "nodeId");
        this.host = requireText(host, "host");

        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException(
                    "Port must be between 1 and 65535"
            );
        }

        this.port = port;
        this.active = true;
    }

    private static String requireText(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName + " cannot be null");

        if (value.isBlank()) {
            throw new IllegalArgumentException(
                    fieldName + " cannot be blank"
            );
        }

        return value;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public boolean isActive() {
        return active;
    }

    public void activate() {
        active = true;
    }

    public void deactivate() {
        active = false;
    }

    @Override
    public String toString() {
        return "StorageNode{" +
                "nodeId='" + nodeId + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", active=" + active +
                '}';
    }
}