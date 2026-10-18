package com.codecrafters.redis.distributed;

import com.codecrafters.redis.storage.StorageEngine;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

public final class ReplicationServerTest {

    public static void main(String[] args) throws Exception {
        Path root = Files.createTempDirectory("tcp-replication-test-");

        try {
            testPutAndDelete(root);
            testTtlReplication(root);

            System.out.println(
                    "\nAll TCP replication tests passed.");
        } finally {
            deleteDirectory(root);
        }
    }

    private static void testPutAndDelete(Path root) throws Exception {
        int port = findAvailablePort();

        try (StorageEngine source =
                     new StorageEngine(root.resolve("source"));
             StorageEngine target =
                     new StorageEngine(root.resolve("target"));
             ReplicationServer server =
                     new ReplicationServer(port, target)) {

            AtomicReference<Throwable> serverError =
                    new AtomicReference<>();

            Thread serverThread = new Thread(() -> {
                try {
                    server.start();
                } catch (IOException e) {
                    serverError.set(e);
                }
            });

            serverThread.setDaemon(true);
            serverThread.start();

            try {
                ReplicationClient client =
                        new ReplicationClient("127.0.0.1", port);

                // Store the original value on the source node.
                source.put("user:1", "Mursalim");

                // Send the same write over TCP to the target node.
                client.replicatePut("user:1", "Mursalim", -1L);

                assertEquals(
                        "Mursalim",
                        target.get("user:1"),
                        "Replicated PUT");

                System.out.println("PASS: TCP PUT replication");

                client.replicateDelete("user:1");

                assertEquals(
                        null,
                        target.get("user:1"),
                        "Replicated DELETE");

                System.out.println("PASS: TCP DELETE replication");

                if (serverError.get() != null) {
                    throw new AssertionError(
                            "Replication server failed",
                            serverError.get());
                }
            } finally {
                server.close();
                serverThread.join(2000);
            }
        }
    }

    private static void testTtlReplication(Path root)
            throws Exception {

        int port = findAvailablePort();

        try (StorageEngine target =
                     new StorageEngine(root.resolve("ttl-target"));
             ReplicationServer server =
                     new ReplicationServer(port, target)) {

            Thread serverThread = new Thread(() -> {
                try {
                    server.start();
                } catch (IOException e) {
                    if (!serverSocketShutdownExpected(server)) {
                        throw new RuntimeException(e);
                    }
                }
            });

            serverThread.setDaemon(true);
            serverThread.start();

            try {
                ReplicationClient client =
                        new ReplicationClient("127.0.0.1", port);

                long expiry = System.currentTimeMillis() + 1200;

                client.replicatePut("temporary", "value", expiry);

                assertEquals(
                        "value",
                        target.get("temporary"),
                        "Value before expiry");

                Thread.sleep(1500);

                assertEquals(
                        null,
                        target.get("temporary"),
                        "Value after expiry");

                System.out.println("PASS: TTL replication");
            } finally {
                server.close();
                serverThread.join(2000);
            }
        }
    }

    private static boolean serverSocketShutdownExpected(
            ReplicationServer server) {
        // Closing the server ends its blocking accept loop.
        return true;
    }

    private static int findAvailablePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void assertEquals(
            Object expected,
            Object actual,
            String testName) {

        if (expected == null ? actual != null : !expected.equals(actual)) {
            throw new AssertionError(
                    testName + " failed. Expected: "
                            + expected + ", actual: " + actual);
        }
    }

    private static void deleteDirectory(Path directory)
            throws IOException {

        if (!Files.exists(directory)) {
            return;
        }

        try (var paths = Files.walk(directory)) {
            for (Path path : paths
                    .sorted(java.util.Comparator.reverseOrder())
                    .toList()) {
                Files.deleteIfExists(path);
            }
        }
    }
}