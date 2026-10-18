package com.codecrafters.redis.distributed;

import com.codecrafters.redis.storage.StorageEngine;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Integration tests for the TCP replication path.
 */
public final class ReplicationIntegrationTest {

    private ReplicationIntegrationTest() {
    }

    public static void main(String[] args) throws Exception {
        Path root = Files.createTempDirectory("repl-integ-");
        try {
            testMultiplePuts(root);
            testPutThenDelete(root);
            testOverwrite(root);
            testTtlReplication(root);
            testReplicaPersistence(root);
            testUnreachableServer(root);
            testInterleavedOps(root);

            System.out.println(
                    "\nAll replication integration tests passed.");
        } finally {
            deleteDirectory(root);
        }
    }

    private static void testMultiplePuts(Path root) throws Exception {
        Path targetDir = root.resolve("multi-put-target");
        int port = findFreePort();
        StorageEngine target = new StorageEngine(targetDir);
        ReplicationServer server = new ReplicationServer(port, target);
        Thread serverThread = startServer(server);

        try {
            Thread.sleep(100);
            ReplicationClient client =
                    new ReplicationClient("127.0.0.1", port);

            client.replicatePut("key1", "val1", -1L);
            client.replicatePut("key2", "val2", -1L);
            client.replicatePut("key3", "val3", -1L);

            check("val1".equals(target.get("key1")),
                    "key1 should be replicated");
            check("val2".equals(target.get("key2")),
                    "key2 should be replicated");
            check("val3".equals(target.get("key3")),
                    "key3 should be replicated");

            System.out.println("PASS: Multiple PUTs replicated");
        } finally {
            server.close();
            target.close();
            serverThread.join(2000);
        }
    }

    private static void testPutThenDelete(Path root) throws Exception {
        Path targetDir = root.resolve("put-del-target");
        int port = findFreePort();
        StorageEngine target = new StorageEngine(targetDir);
        ReplicationServer server = new ReplicationServer(port, target);
        Thread serverThread = startServer(server);

        try {
            Thread.sleep(100);
            ReplicationClient client =
                    new ReplicationClient("127.0.0.1", port);

            client.replicatePut("key", "value", -1L);
            check("value".equals(target.get("key")),
                    "Key should be present after PUT");

            client.replicateDelete("key");
            check(target.get("key") == null,
                    "Key should be null after DELETE");

            System.out.println("PASS: PUT then DELETE replicated");
        } finally {
            server.close();
            target.close();
            serverThread.join(2000);
        }
    }

    private static void testOverwrite(Path root) throws Exception {
        Path targetDir = root.resolve("overwrite-target");
        int port = findFreePort();
        StorageEngine target = new StorageEngine(targetDir);
        ReplicationServer server = new ReplicationServer(port, target);
        Thread serverThread = startServer(server);

        try {
            Thread.sleep(100);
            ReplicationClient client =
                    new ReplicationClient("127.0.0.1", port);

            client.replicatePut("key", "first", -1L);
            client.replicatePut("key", "second", -1L);

            check("second".equals(target.get("key")),
                    "Replica should have latest value");

            System.out.println("PASS: Overwrite replicated");
        } finally {
            server.close();
            target.close();
            serverThread.join(2000);
        }
    }

    private static void testTtlReplication(Path root) throws Exception {
        Path targetDir = root.resolve("ttl-target");
        int port = findFreePort();
        StorageEngine target = new StorageEngine(targetDir);
        ReplicationServer server = new ReplicationServer(port, target);
        Thread serverThread = startServer(server);

        try {
            Thread.sleep(100);
            ReplicationClient client =
                    new ReplicationClient("127.0.0.1", port);

            long expiry = System.currentTimeMillis() + 800;
            client.replicatePut("temp", "alive", expiry);

            check("alive".equals(target.get("temp")),
                    "TTL key should be present before expiry");

            Thread.sleep(1000);

            check(target.get("temp") == null,
                    "TTL key should be null after expiry");

            System.out.println("PASS: TTL replication");
        } finally {
            server.close();
            target.close();
            serverThread.join(2000);
        }
    }

    private static void testReplicaPersistence(Path root) throws Exception {
        Path targetDir = root.resolve("persist-target");
        int port = findFreePort();

        // Phase 1: replicate data.
        StorageEngine target = new StorageEngine(targetDir);
        ReplicationServer server = new ReplicationServer(port, target);
        Thread serverThread = startServer(server);

        try {
            Thread.sleep(100);
            ReplicationClient client =
                    new ReplicationClient("127.0.0.1", port);

            client.replicatePut("persist-key", "persist-value", -1L);
            target.flush();
        } finally {
            server.close();
            target.close();
            serverThread.join(2000);
        }

        // Phase 2: reopen storage and verify.
        try (StorageEngine reopened = new StorageEngine(targetDir)) {
            check("persist-value".equals(reopened.get("persist-key")),
                    "Replicated value should survive restart");
        }

        System.out.println("PASS: Replica persistence after restart");
    }

    private static void testUnreachableServer(Path root) throws Exception {
        // Try to replicate to a port with no server running.
        int port = findFreePort();
        ReplicationClient client =
                new ReplicationClient("127.0.0.1", port);

        boolean failed = false;
        try {
            client.replicatePut("key", "value", -1L);
        } catch (IOException e) {
            failed = true;
        }

        check(failed,
                "Replication to unreachable server should throw IOException");
        System.out.println(
                "PASS: Unreachable server throws IOException");
    }

    private static void testInterleavedOps(Path root) throws Exception {
        Path targetDir = root.resolve("interleaved-target");
        int port = findFreePort();
        StorageEngine target = new StorageEngine(targetDir);
        ReplicationServer server = new ReplicationServer(port, target);
        Thread serverThread = startServer(server);

        try {
            Thread.sleep(100);
            ReplicationClient client =
                    new ReplicationClient("127.0.0.1", port);

            client.replicatePut("a", "1", -1L);
            client.replicatePut("b", "2", -1L);
            client.replicateDelete("a");
            client.replicatePut("c", "3", -1L);
            client.replicatePut("b", "updated", -1L);
            client.replicateDelete("c");

            check(target.get("a") == null, "a should be deleted");
            check("updated".equals(target.get("b")),
                    "b should have latest value");
            check(target.get("c") == null, "c should be deleted");

            System.out.println("PASS: Interleaved operations");
        } finally {
            server.close();
            target.close();
            serverThread.join(2000);
        }
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static Thread startServer(ReplicationServer server) {
        Thread t = new Thread(() -> {
            try {
                server.start();
            } catch (IOException e) {
                // Server stopped.
            }
        });
        t.setDaemon(true);
        t.start();
        return t;
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static void deleteDirectory(Path dir) throws Exception {
        if (Files.exists(dir)) {
            try (var paths = Files.walk(dir)) {
                paths.sorted(Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            } catch (Exception e) {
                                // Best effort cleanup.
                            }
                        });
            }
        }
    }
}
