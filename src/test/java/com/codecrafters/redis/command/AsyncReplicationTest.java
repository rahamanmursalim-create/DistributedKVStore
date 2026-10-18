package com.codecrafters.redis.command;

import com.codecrafters.redis.ReplicationMode;
import com.codecrafters.redis.distributed.ReplicationClient;
import com.codecrafters.redis.distributed.ReplicationServer;
import com.codecrafters.redis.protocol.RespValue;
import com.codecrafters.redis.storage.StorageEngine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AsyncReplicationTest {

    private static final Path PRIMARY_DIR = Path.of("test-data", "async-rep-primary");
    private static final Path REPLICA_DIR = Path.of("test-data", "async-rep-replica");
    private static final int REPLICA_PORT = 16391;

    public static void main(String[] args) throws Exception {
        cleanDirectory(PRIMARY_DIR);
        cleanDirectory(REPLICA_DIR);
        Files.createDirectories(PRIMARY_DIR);
        Files.createDirectories(REPLICA_DIR);

        try (StorageEngine primaryStorage = new StorageEngine(PRIMARY_DIR);
             StorageEngine replicaStorage = new StorageEngine(REPLICA_DIR);
             ReplicationServer replicaServer = new ReplicationServer(REPLICA_PORT, replicaStorage)) {

            ExecutorService serverExecutor = Executors.newSingleThreadExecutor();
            serverExecutor.submit(() -> {
                try {
                    replicaServer.start();
                } catch (IOException ignored) {}
            });

            Thread.sleep(200); // Allow replica server to bind

            ReplicationClient replicationClient = new ReplicationClient("127.0.0.1", REPLICA_PORT);

            try (CommandHandler handler = new CommandHandler(primaryStorage, replicationClient, ReplicationMode.ASYNC)) {
                assert handler.getReplicationMode() == ReplicationMode.ASYNC : "Expected ASYNC mode";

                testAsyncPutAndDrain(handler, primaryStorage, replicaStorage);
                testAsyncDeleteAndDrain(handler, primaryStorage, replicaStorage);
                testAsyncBurstOperations(handler, primaryStorage, replicaStorage);
            }

            serverExecutor.shutdownNow();
            serverExecutor.awaitTermination(1, TimeUnit.SECONDS);
        } finally {
            cleanDirectory(PRIMARY_DIR);
            cleanDirectory(REPLICA_DIR);
        }

        System.out.println("AsyncReplicationTest passed all tests.");
    }

    private static void testAsyncPutAndDrain(
            CommandHandler handler,
            StorageEngine primary,
            StorageEngine replica) throws Exception {

        RespValue res = handler.execute(Command.of("SET", "key1", "val1"));
        assert "OK".equals(res.getStringValue()) : "Expected OK response";

        // Locally visible immediately
        assert "val1".equals(primary.get("key1")) : "Should be in primary immediately";

        // Wait for async queue to flush
        boolean flushed = handler.flushReplicationQueue(2000);
        assert flushed : "Replication queue should flush successfully";
        assert handler.getPendingReplicationCount() == 0 : "Pending count should be 0";

        // Verified on replica
        assert "val1".equals(replica.get("key1")) : "Replica should have received val1";
    }

    private static void testAsyncDeleteAndDrain(
            CommandHandler handler,
            StorageEngine primary,
            StorageEngine replica) throws Exception {

        RespValue res = handler.execute(Command.of("DEL", "key1"));
        assert res.getIntegerValue() == 1L : "Expected 1 deleted key";

        assert primary.get("key1") == null : "Primary should be null after delete";

        boolean flushed = handler.flushReplicationQueue(2000);
        assert flushed : "Replication queue should flush";
        assert handler.getPendingReplicationCount() == 0 : "Lag should be 0";

        assert replica.get("key1") == null : "Replica should have deleted key1";
    }

    private static void testAsyncBurstOperations(
            CommandHandler handler,
            StorageEngine primary,
            StorageEngine replica) throws Exception {

        int burstSize = 20;
        for (int i = 0; i < burstSize; i++) {
            handler.execute(Command.of("SET", "burst:" + i, "data:" + i));
        }

        // Flushed
        boolean flushed = handler.flushReplicationQueue(5000);
        assert flushed : "Burst replication should finish within deadline";
        assert handler.getPendingReplicationCount() == 0;

        for (int i = 0; i < burstSize; i++) {
            assert ("data:" + i).equals(replica.get("burst:" + i)) : "Mismatch at index " + i;
        }
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
