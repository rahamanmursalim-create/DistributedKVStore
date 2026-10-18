package com.codecrafters.redis.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

/**
 * Edge-case tests for StorageEngine.
 */
public final class StorageEngineEdgeCaseTest {

    private StorageEngineEdgeCaseTest() {
    }

    public static void main(String[] args) throws Exception {
        Path testDir = Files.createTempDirectory("storage-engine-edge-");

        try {
            testNullKeyRejected(testDir);
            testNullValueRejected(testDir);
            testOverwriteKey(testDir);
            testDeleteNonexistent(testDir);
            testGetNullKey(testDir);
            testEmptyFlush(testDir);
            testMultipleUpdatesAcrossFlushes(testDir);
            testDeleteAndReinsert(testDir);
            testCompactSingleSSTable(testDir);
            testClosedEngineRejects(testDir);
            testLargeKeyAndValue(testDir);
            testEmptyStringKey(testDir);

            System.out.println("ALL STORAGE ENGINE EDGE CASE TESTS PASSED");
        } finally {
            deleteDirectory(testDir);
        }
    }

    private static void testNullKeyRejected(Path root) throws Exception {
        Path dir = root.resolve("null-key");
        try (StorageEngine engine = new StorageEngine(dir)) {
            boolean rejected = false;
            try {
                engine.put(null, "value");
            } catch (IllegalArgumentException e) {
                rejected = true;
            }
            check(rejected, "Null key should be rejected");
        }
        System.out.println("TEST 1 PASSED: Null key rejected");
    }

    private static void testNullValueRejected(Path root) throws Exception {
        Path dir = root.resolve("null-value");
        try (StorageEngine engine = new StorageEngine(dir)) {
            boolean rejected = false;
            try {
                engine.put("key", null);
            } catch (IllegalArgumentException e) {
                rejected = true;
            }
            check(rejected, "Null value should be rejected");
        }
        System.out.println("TEST 2 PASSED: Null value rejected");
    }

    private static void testOverwriteKey(Path root) throws Exception {
        Path dir = root.resolve("overwrite");
        try (StorageEngine engine = new StorageEngine(dir)) {
            engine.put("key", "first");
            check("first".equals(engine.get("key")),
                    "First put should be visible");
            engine.put("key", "second");
            check("second".equals(engine.get("key")),
                    "Overwrite should return latest value");
        }
        System.out.println("TEST 3 PASSED: Overwrite existing key");
    }

    private static void testDeleteNonexistent(Path root) throws Exception {
        Path dir = root.resolve("delete-missing");
        try (StorageEngine engine = new StorageEngine(dir)) {
            boolean result = engine.delete("nonexistent");
            check(!result, "Deleting nonexistent key should return false");
        }
        System.out.println("TEST 4 PASSED: Delete nonexistent key returns false");
    }

    private static void testGetNullKey(Path root) throws Exception {
        Path dir = root.resolve("get-null");
        try (StorageEngine engine = new StorageEngine(dir)) {
            String result = engine.get(null);
            check(result == null, "Get null key should return null");
        }
        System.out.println("TEST 5 PASSED: Get null key returns null");
    }

    private static void testEmptyFlush(Path root) throws Exception {
        Path dir = root.resolve("empty-flush");
        try (StorageEngine engine = new StorageEngine(dir)) {
            engine.flush();
            check(engine.sstableCount() == 0,
                    "Empty flush should not create an SSTable");
        }
        System.out.println("TEST 6 PASSED: Empty flush is a no-op");
    }

    private static void testMultipleUpdatesAcrossFlushes(Path root)
            throws Exception {
        Path dir = root.resolve("multi-update");
        try (StorageEngine engine = new StorageEngine(dir)) {
            engine.put("key", "v1");
            engine.flush();
            engine.put("key", "v2");
            engine.flush();
            engine.put("key", "v3");
            engine.flush();
            check("v3".equals(engine.get("key")),
                    "Latest value should be visible across flushes");

            // Verify after compaction too.
            engine.compact();
            check("v3".equals(engine.get("key")),
                    "Latest value should survive compaction");
            check(engine.sstableCount() == 1,
                    "Should have one SSTable after compaction");
        }
        System.out.println(
                "TEST 7 PASSED: Multiple updates across flushes");
    }

    private static void testDeleteAndReinsert(Path root) throws Exception {
        Path dir = root.resolve("delete-reinsert");
        try (StorageEngine engine = new StorageEngine(dir)) {
            engine.put("key", "original");
            engine.flush();
            engine.delete("key");
            engine.flush();
            check(engine.get("key") == null,
                    "Deleted key should be null");

            engine.put("key", "reinserted");
            check("reinserted".equals(engine.get("key")),
                    "Reinserted key should be visible");
            engine.flush();
            check("reinserted".equals(engine.get("key")),
                    "Reinserted key should survive flush");
        }

        // Verify after restart.
        try (StorageEngine engine = new StorageEngine(dir)) {
            check("reinserted".equals(engine.get("key")),
                    "Reinserted key should survive restart");
        }
        System.out.println("TEST 8 PASSED: Delete and reinsert");
    }

    private static void testCompactSingleSSTable(Path root) throws Exception {
        Path dir = root.resolve("compact-single");
        try (StorageEngine engine = new StorageEngine(dir)) {
            engine.put("key", "value");
            engine.flush();
            check(engine.sstableCount() == 1,
                    "Should have one SSTable before compact");
            engine.compact();
            check(engine.sstableCount() == 1,
                    "Single SSTable compaction is a no-op");
            check("value".equals(engine.get("key")),
                    "Value should survive no-op compaction");
        }
        System.out.println(
                "TEST 9 PASSED: Compact with single SSTable is a no-op");
    }

    private static void testClosedEngineRejects(Path root) throws Exception {
        Path dir = root.resolve("closed-engine");
        StorageEngine engine = new StorageEngine(dir);
        engine.put("before", "close");
        engine.close();

        boolean putRejected = false;
        try {
            engine.put("after", "close");
        } catch (IOException e) {
            putRejected = true;
        }
        check(putRejected, "Put on closed engine should throw IOException");

        boolean deleteRejected = false;
        try {
            engine.delete("before");
        } catch (IOException e) {
            deleteRejected = true;
        }
        check(deleteRejected,
                "Delete on closed engine should throw IOException");

        System.out.println(
                "TEST 10 PASSED: Closed engine rejects operations");
    }

    private static void testLargeKeyAndValue(Path root) throws Exception {
        Path dir = root.resolve("large-kv");
        String largeKey = "K".repeat(1000);
        String largeValue = "V".repeat(1000);

        try (StorageEngine engine = new StorageEngine(dir)) {
            engine.put(largeKey, largeValue);
            check(largeValue.equals(engine.get(largeKey)),
                    "Large key-value should be readable");
            engine.flush();
            check(largeValue.equals(engine.get(largeKey)),
                    "Large key-value should survive flush");
        }

        try (StorageEngine engine = new StorageEngine(dir)) {
            check(largeValue.equals(engine.get(largeKey)),
                    "Large key-value should survive restart");
        }
        System.out.println(
                "TEST 11 PASSED: Large key and value survive flush and restart");
    }

    private static void testEmptyStringKey(Path root) throws Exception {
        Path dir = root.resolve("empty-key");
        try (StorageEngine engine = new StorageEngine(dir)) {
            engine.put("", "empty-key-value");
            check("empty-key-value".equals(engine.get("")),
                    "Empty string key should work");
            engine.flush();
            check("empty-key-value".equals(engine.get("")),
                    "Empty key should survive flush");
            boolean deleted = engine.delete("");
            check(deleted, "Empty key should be deletable");
            check(engine.get("") == null,
                    "Deleted empty key should return null");
        }
        System.out.println("TEST 12 PASSED: Empty string key works correctly");
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
