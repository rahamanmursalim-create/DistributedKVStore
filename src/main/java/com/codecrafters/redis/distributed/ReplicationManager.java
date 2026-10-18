package com.codecrafters.redis.distributed;

import com.codecrafters.redis.storage.StorageEngine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Manages replication and quorum read/write operations across both local
 * StorageEngines and remote network nodes via ReplicationClient.
 */
public final class ReplicationManager {

    private final Map<String, NodeEndpoint> endpoints = new LinkedHashMap<>();
    private final int replicationFactor;

    public ReplicationManager(int replicationFactor) {
        if (replicationFactor < 1) {
            throw new IllegalArgumentException(
                    "Replication factor must be at least 1");
        }

        this.replicationFactor = replicationFactor;
    }

    public synchronized void registerEngine(
            String nodeId,
            StorageEngine engine) {

        Objects.requireNonNull(nodeId, "nodeId cannot be null");
        Objects.requireNonNull(engine, "engine cannot be null");

        if (nodeId.isBlank()) {
            throw new IllegalArgumentException(
                    "Node ID cannot be blank");
        }

        if (endpoints.containsKey(nodeId)) {
            throw new IllegalArgumentException(
                    "Engine already registered for node: " + nodeId);
        }

        endpoints.put(nodeId, new LocalEngineEndpoint(engine));
    }

    public synchronized void registerRemoteClient(
            String nodeId,
            ReplicationClient client) {

        Objects.requireNonNull(nodeId, "nodeId cannot be null");
        Objects.requireNonNull(client, "client cannot be null");

        if (nodeId.isBlank()) {
            throw new IllegalArgumentException(
                    "Node ID cannot be blank");
        }

        if (endpoints.containsKey(nodeId)) {
            throw new IllegalArgumentException(
                    "Endpoint already registered for node: " + nodeId);
        }

        endpoints.put(nodeId, new RemoteClientEndpoint(client));
    }

    public synchronized void unregisterEngine(String nodeId) {
        endpoints.remove(nodeId);
    }

    public synchronized void unregisterClient(String nodeId) {
        endpoints.remove(nodeId);
    }

    public synchronized int getRegisteredNodeCount() {
        return endpoints.size();
    }

    public synchronized int getReplicationFactor() {
        return replicationFactor;
    }

    public synchronized void replicatePut(
            List<String> replicaNodeIds,
            String key,
            String value,
            long expiryEpochMillis) throws IOException {

        validateKey(key);
        Objects.requireNonNull(value, "value cannot be null");

        List<NodeEndpoint> targets = resolveTargets(replicaNodeIds);

        IOException failure = null;
        int successfulWrites = 0;

        for (NodeEndpoint endpoint : targets) {
            try {
                endpoint.put(key, value, expiryEpochMillis);
                successfulWrites++;
            } catch (IOException e) {
                if (failure == null) {
                    failure = new IOException(
                            "Replication failed on one or more targets", e);
                } else {
                    failure.addSuppressed(e);
                }
            }
        }

        int required = Math.min(replicationFactor, targets.size());

        if (successfulWrites < required) {
            if (failure != null) {
                throw failure;
            }

            throw new IOException(
                    "Insufficient successful replicas: "
                            + successfulWrites + "/" + required);
        }
    }

    public synchronized void replicateDelete(
            List<String> replicaNodeIds,
            String key) throws IOException {

        validateKey(key);

        List<NodeEndpoint> targets = resolveTargets(replicaNodeIds);

        IOException failure = null;
        int successfulDeletes = 0;

        for (NodeEndpoint endpoint : targets) {
            try {
                endpoint.delete(key);
                successfulDeletes++;
            } catch (IOException e) {
                if (failure == null) {
                    failure = new IOException(
                            "Delete replication failed on one or more targets",
                            e);
                } else {
                    failure.addSuppressed(e);
                }
            }
        }

        int required = Math.min(replicationFactor, targets.size());

        if (successfulDeletes < required) {
            if (failure != null) {
                throw failure;
            }

            throw new IOException(
                    "Insufficient successful replicas: "
                            + successfulDeletes + "/" + required);
        }
    }

    public synchronized String readFromNode(
            String nodeId,
            String key) throws IOException {

        Objects.requireNonNull(nodeId, "nodeId cannot be null");
        validateKey(key);

        NodeEndpoint endpoint = endpoints.get(nodeId);

        if (endpoint == null) {
            throw new IllegalArgumentException(
                    "No storage engine or remote client registered for node: " + nodeId);
        }

        return endpoint.get(key);
    }

    private List<NodeEndpoint> resolveTargets(
            List<String> replicaNodeIds) {

        Objects.requireNonNull(
                replicaNodeIds, "replicaNodeIds cannot be null");

        if (replicaNodeIds.isEmpty()) {
            throw new IllegalArgumentException(
                    "At least one target node is required");
        }

        List<NodeEndpoint> targets = new ArrayList<>();
        List<String> seenIds = new ArrayList<>();

        for (String nodeId : replicaNodeIds) {
            Objects.requireNonNull(nodeId, "node ID cannot be null");

            NodeEndpoint endpoint = endpoints.get(nodeId);

            if (endpoint == null) {
                throw new IllegalArgumentException(
                        "No storage engine registered for node: " + nodeId);
            }

            if (seenIds.contains(nodeId)) {
                throw new IllegalArgumentException(
                        "Duplicate replication target: " + nodeId);
            }

            seenIds.add(nodeId);
            targets.add(endpoint);
        }

        return targets;
    }

    private static void validateKey(String key) {
        Objects.requireNonNull(key, "key cannot be null");

        if (key.isBlank()) {
            throw new IllegalArgumentException("Key cannot be blank");
        }
    }

    private interface NodeEndpoint {
        void put(String key, String value, long expiryEpochMillis) throws IOException;
        void delete(String key) throws IOException;
        String get(String key) throws IOException;
    }

    private static final class LocalEngineEndpoint implements NodeEndpoint {
        private final StorageEngine engine;

        LocalEngineEndpoint(StorageEngine engine) {
            this.engine = engine;
        }

        @Override
        public void put(String key, String value, long expiryEpochMillis) throws IOException {
            engine.put(key, value, expiryEpochMillis);
        }

        @Override
        public void delete(String key) throws IOException {
            engine.delete(key);
        }

        @Override
        public String get(String key) throws IOException {
            return engine.get(key);
        }
    }

    private static final class RemoteClientEndpoint implements NodeEndpoint {
        private final ReplicationClient client;

        RemoteClientEndpoint(ReplicationClient client) {
            this.client = client;
        }

        @Override
        public void put(String key, String value, long expiryEpochMillis) throws IOException {
            client.replicatePut(key, value, expiryEpochMillis);
        }

        @Override
        public void delete(String key) throws IOException {
            client.replicateDelete(key);
        }

        @Override
        public String get(String key) throws IOException {
            return client.replicateGet(key);
        }
    }
}