package com.codecrafters.redis.distributed;

import com.codecrafters.redis.storage.StorageEngine;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;

public final class ReplicationManagerTest {

    private static int passed = 0;

    public static void main(String[] args) throws Exception {
        passed = 0;

        testWriteReplication();
        testDeleteReplication();
        testMissingNode();
        testDuplicateTarget();
        testInvalidReplicationFactor();

        System.out.println();
        System.out.println(
                "All ReplicationManager tests passed: " + passed);
    }

    private static void testWriteReplication() throws Exception {
        Path root = Files.createTempDirectory("replication-write-");

        try (StorageEngine primary =
                     new StorageEngine(root.resolve("primary"));
             StorageEngine replica =
                     new StorageEngine(root.resolve("replica"))) {

            ReplicationManager manager = new ReplicationManager(2);
            manager.registerEngine("node-A", primary);
            manager.registerEngine("node-B", replica);

            manager.replicatePut(
                    List.of("node-A", "node-B"),
                    "user:1",
                    "Mursalim",
                    -1L);

            assertEquals("Mursalim", primary.get("user:1"));
            assertEquals("Mursalim", replica.get("user:1"));

            pass("Write replicated to both nodes");
        } finally {
            deleteDirectory(root);
        }
    }

    private static void testDeleteReplication() throws Exception {
        Path root = Files.createTempDirectory("replication-delete-");

        try (StorageEngine primary =
                     new StorageEngine(root.resolve("primary"));
             StorageEngine replica =
                     new StorageEngine(root.resolve("replica"))) {

            primary.put("session:1", "active");
            replica.put("session:1", "active");

            ReplicationManager manager = new ReplicationManager(2);
            manager.registerEngine("node-A", primary);
            manager.registerEngine("node-B", replica);

            manager.replicateDelete(
                    List.of("node-A", "node-B"),
                    "session:1");

            assertEquals(null, primary.get("session:1"));
            assertEquals(null, replica.get("session:1"));

            pass("Delete replicated to both nodes");
        } finally {
            deleteDirectory(root);
        }
    }

    private static void testMissingNode() throws Exception {
        Path root = Files.createTempDirectory("replication-missing-");

        try (StorageEngine engine =
                     new StorageEngine(root.resolve("primary"))) {

            ReplicationManager manager = new ReplicationManager(1);
            manager.registerEngine("node-A", engine);

            expectThrows(
                    IllegalArgumentException.class,
                    () -> manager.replicatePut(
                            List.of("node-UNKNOWN"),
                            "key",
                            "value",
                            -1L));

            pass("Unknown node rejected");
        } finally {
            deleteDirectory(root);
        }
    }

    private static void testDuplicateTarget() throws Exception {
        Path root = Files.createTempDirectory("replication-duplicate-");

        try (StorageEngine engine =
                     new StorageEngine(root.resolve("primary"))) {

            ReplicationManager manager = new ReplicationManager(1);
            manager.registerEngine("node-A", engine);

            expectThrows(
                    IllegalArgumentException.class,
                    () -> manager.replicatePut(
                            List.of("node-A", "node-A"),
                            "key",
                            "value",
                            -1L));

            pass("Duplicate replication target rejected");
        } finally {
            deleteDirectory(root);
        }
    }

    private static void testInvalidReplicationFactor() {
        expectThrows(
                IllegalArgumentException.class,
                () -> new ReplicationManager(0));

        pass("Invalid replication factor rejected");
    }

    private static void assertEquals(
            Object expected,
            Object actual) {

        if (expected == null ? actual != null : !expected.equals(actual)) {
            throw new AssertionError(
                    "Expected: " + expected + ", actual: " + actual);
        }
    }

    private static void expectThrows(
            Class<? extends Throwable> expected,
            ThrowingRunnable action) {

        try {
            action.run();
        } catch (Throwable actual) {
            if (expected.isInstance(actual)) {
                return;
            }

            throw new AssertionError(
                    "Expected " + expected.getSimpleName()
                            + " but received " + actual,
                    actual);
        }

        throw new AssertionError(
                "Expected exception: " + expected.getSimpleName());
    }

    private static void pass(String message) {
        passed++;
        System.out.println("PASS: " + message);
    }

    private static void deleteDirectory(Path root) throws Exception {
        if (!Files.exists(root)) {
            return;
        }

        try (var paths = Files.walk(root)) {
            for (Path path : paths
                    .sorted(Comparator.reverseOrder())
                    .toList()) {
                Files.deleteIfExists(path);
            }
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}