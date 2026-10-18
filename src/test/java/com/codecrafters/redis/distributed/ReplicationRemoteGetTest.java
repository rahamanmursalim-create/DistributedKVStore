package com.codecrafters.redis.distributed;

import com.codecrafters.redis.storage.StorageEngine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ReplicationRemoteGetTest {

    private static final Path TEST_DIR = Path.of("test-data", "replication-remote-get-test");
    private static final int PORT = 16390;

    public static void main(String[] args) throws Exception {
        cleanDirectory(TEST_DIR);
        Files.createDirectories(TEST_DIR);

        try (StorageEngine engine = new StorageEngine(TEST_DIR);
             ReplicationServer server = new ReplicationServer(PORT, engine)) {

            ExecutorService serverExecutor = Executors.newSingleThreadExecutor();
            serverExecutor.submit(() -> {
                try {
                    server.start();
                } catch (IOException ignored) {}
            });

            Thread.sleep(200); // Allow server socket to bind

            ReplicationClient client = new ReplicationClient("127.0.0.1", PORT);
            ReplicationManager manager = new ReplicationManager(1);
            manager.registerRemoteClient("node-1", client);

            testRemoteGetExistingKey(engine, client, manager);
            testRemoteGetNonExistentKey(client, manager);
            testRemoteGetDeletedKey(engine, client, manager);
            testRemoteGetExpiredKey(engine, client, manager);

            serverExecutor.shutdownNow();
            serverExecutor.awaitTermination(1, TimeUnit.SECONDS);
        } finally {
            cleanDirectory(TEST_DIR);
        }

        System.out.println("ReplicationRemoteGetTest passed all tests.");
    }

    private static void testRemoteGetExistingKey(
            StorageEngine engine,
            ReplicationClient client,
            ReplicationManager manager) throws Exception {

        engine.put("user:1", "alice");

        String valFromClient = client.replicateGet("user:1");
        assert "alice".equals(valFromClient) : "Expected 'alice', got " + valFromClient;

        String valFromManager = manager.readFromNode("node-1", "user:1");
        assert "alice".equals(valFromManager) : "Expected 'alice' via manager, got " + valFromManager;
    }

    private static void testRemoteGetNonExistentKey(
            ReplicationClient client,
            ReplicationManager manager) throws Exception {

        String valFromClient = client.replicateGet("nonexistent");
        assert valFromClient == null : "Expected null for nonexistent key, got " + valFromClient;

        String valFromManager = manager.readFromNode("node-1", "nonexistent");
        assert valFromManager == null : "Expected null via manager for nonexistent key, got " + valFromManager;
    }

    private static void testRemoteGetDeletedKey(
            StorageEngine engine,
            ReplicationClient client,
            ReplicationManager manager) throws Exception {

        engine.put("tempKey", "toDelete");
        assert "toDelete".equals(client.replicateGet("tempKey"));

        engine.delete("tempKey");
        assert client.replicateGet("tempKey") == null : "Expected null after deletion";
        assert manager.readFromNode("node-1", "tempKey") == null : "Expected null via manager after deletion";
    }

    private static void testRemoteGetExpiredKey(
            StorageEngine engine,
            ReplicationClient client,
            ReplicationManager manager) throws Exception {

        long expireAt = System.currentTimeMillis() + 100;
        engine.put("expiringKey", "quickValue", expireAt);

        assert "quickValue".equals(client.replicateGet("expiringKey")) : "Should exist before expiry";

        Thread.sleep(150);

        assert client.replicateGet("expiringKey") == null : "Expected null after key expiration";
        assert manager.readFromNode("node-1", "expiringKey") == null : "Expected null via manager after key expiration";
    }

    private static void cleanDirectory(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }

        List<Path> paths;
        try (var stream = Files.walk(dir)) {
            paths = stream.sorted(Comparator.reverseOrder()).toList();
        }

        for (Path path : paths) {
            Files.deleteIfExists(path);
        }
    }
}
