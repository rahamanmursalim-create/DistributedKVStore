
package com.codecrafters.redis.cluster;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class ClusterMembership {

    private final Map<String, NodeStatus> nodes =
            new ConcurrentHashMap<>();

    public void addNode(String nodeId) {
        validateNodeId(nodeId);

        NodeStatus previous =
                nodes.putIfAbsent(nodeId, NodeStatus.STARTING);

        if (previous != null) {
            throw new IllegalArgumentException(
                    "Node already exists: " + nodeId);
        }
    }

    public boolean removeNode(String nodeId) {
        validateNodeId(nodeId);
        return nodes.remove(nodeId) != null;
    }

    public boolean updateStatus(String nodeId, NodeStatus status) {
        validateNodeId(nodeId);

        if (status == null) {
            throw new IllegalArgumentException(
                    "Status cannot be null");
        }

        return nodes.replace(nodeId, status) != null;
    }

    public Optional<NodeStatus> getStatus(String nodeId) {
        validateNodeId(nodeId);
        return Optional.ofNullable(nodes.get(nodeId));
    }

    public boolean containsNode(String nodeId) {
        validateNodeId(nodeId);
        return nodes.containsKey(nodeId);
    }

    public int getNodeCount() {
        return nodes.size();
    }

    public Set<String> getNodeIds() {
        return Collections.unmodifiableSet(nodes.keySet());
    }

    public Collection<String> getActiveNodeIds() {
        return nodes.entrySet().stream()
                .filter(entry -> entry.getValue() == NodeStatus.ACTIVE)
                .map(Map.Entry::getKey)
                .toList();
    }

    private void validateNodeId(String nodeId) {
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException(
                    "Node ID cannot be null or blank");
        }
    }
}
