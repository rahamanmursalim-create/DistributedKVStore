package com.codecrafters.redis.distributed;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public final class DistributedNodeRegistry {

    private final Map<String, StorageNode> nodes = new LinkedHashMap<>();

    public synchronized void registerNode(StorageNode node) {
        Objects.requireNonNull(node, "node cannot be null");

        String nodeId = node.getNodeId();

        if (nodes.containsKey(nodeId)) {
            throw new IllegalArgumentException(
                    "Node already registered: " + nodeId
            );
        }

        nodes.put(nodeId, node);
    }

    public synchronized Optional<StorageNode> getNode(String nodeId) {
        return Optional.ofNullable(nodes.get(nodeId));
    }

    public synchronized boolean removeNode(String nodeId) {
        return nodes.remove(nodeId) != null;
    }

    public synchronized boolean containsNode(String nodeId) {
        return nodes.containsKey(nodeId);
    }

    public synchronized int getNodeCount() {
        return nodes.size();
    }

    public synchronized Collection<StorageNode> getAllNodes() {
        return Collections.unmodifiableList(
                new ArrayList<>(nodes.values())
        );
    }

    public synchronized Collection<StorageNode> getActiveNodes() {
        ArrayList<StorageNode> activeNodes = new ArrayList<>();

        for (StorageNode node : nodes.values()) {
            if (node.isActive()) {
                activeNodes.add(node);
            }
        }

        return Collections.unmodifiableList(activeNodes);
    }
}