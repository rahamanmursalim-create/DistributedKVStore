package com.codecrafters.redis.distributed;

import java.util.Collection;
import java.util.Objects;

public final class DistributedKeyRouter {

    private final ConsistentHashRing hashRing;
    private final DistributedNodeRegistry registry;

    public DistributedKeyRouter(
            ConsistentHashRing hashRing,
            DistributedNodeRegistry registry) {

        this.hashRing = Objects.requireNonNull(
                hashRing, "hashRing cannot be null");

        this.registry = Objects.requireNonNull(
                registry, "registry cannot be null");
    }

    public synchronized void addNode(StorageNode node) {
        Objects.requireNonNull(node, "node cannot be null");

        if (registry.containsNode(node.getNodeId())) {
            throw new IllegalArgumentException(
                    "Node already registered: " + node.getNodeId());
        }

        registry.registerNode(node);
        hashRing.addNode(node.getNodeId());
    }

    public synchronized boolean removeNode(String nodeId) {
        Objects.requireNonNull(nodeId, "nodeId cannot be null");

        if (!registry.containsNode(nodeId)) {
            return false;
        }

        hashRing.removeNode(nodeId);
        registry.removeNode(nodeId);
        return true;
    }

    public synchronized StorageNode routeKey(String key) {
        Objects.requireNonNull(key, "key cannot be null");

        if (key.isBlank()) {
            throw new IllegalArgumentException("Key cannot be blank");
        }

        if (registry.getActiveNodes().isEmpty()) {
            throw new IllegalStateException(
                    "No active storage nodes available");
        }

        String selectedNodeId = hashRing.getNode(key);

        StorageNode selectedNode = registry.getNode(selectedNodeId)
                .orElseThrow(() -> new IllegalStateException(
                        "Hash ring references an unregistered node: "
                                + selectedNodeId));

        if (selectedNode.isActive()) {
            return selectedNode;
        }

        // Fallback: walk clockwise along the hash ring preference list to find the next active successor.
        int totalNodes = registry.getNodeCount();
        if (totalNodes > 0) {
            var preference = hashRing.getPreferenceList(key, totalNodes);
            for (String candidateId : preference) {
                var candidate = registry.getNode(candidateId);
                if (candidate.isPresent() && candidate.get().isActive()) {
                    return candidate.get();
                }
            }
        }

        for (StorageNode node : registry.getActiveNodes()) {
            return node;
        }

        throw new IllegalStateException(
                "No active storage nodes available");
    }

    public synchronized Collection<StorageNode> getActiveNodes() {
        return registry.getActiveNodes();
    }
}