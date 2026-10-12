
package com.codecrafters.redis.storage;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;

public class StorageEngineTest {

    public static void main(String[] args) throws Exception {
        Path testDir = Path.of("test-data", "restart-recovery");

        // Clean previous test data.
        if (Files.exists(testDir)) {
            try (var paths = Files.walk(testDir)) {
                paths.sorted(Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
            }
        }

        // TEST 1: Basic write and restart recovery.
        StorageEngine first = new StorageEngine(testDir);

        first.put("name", "Mursalim");
        first.put("language", "Java");
        first.flush();

        System.out.println("First instance: " + first.snapshot());

        StorageEngine second = new StorageEngine(testDir);

        System.out.println("Recovered name: " + second.get("name"));
        System.out.println("Recovered language: " + second.get("language"));
        System.out.println("Recovered SSTables: " + second.sstableCount());

        if (!"Mursalim".equals(second.get("name"))) {
            throw new AssertionError("Name recovery failed");
        }

        if (!"Java".equals(second.get("language"))) {
            throw new AssertionError("Language recovery failed");
        }

        if (second.sstableCount() != 1) {
            throw new AssertionError("Expected one recovered SSTable");
        }

        System.out.println("TEST 1 PASSED: Restart recovery");

        // TEST 2: Delete a key and verify the tombstone.
        boolean deleted = second.delete("name");

        if (!deleted) {
            throw new AssertionError("Expected name to be deleted");
        }

        if (second.get("name") != null) {
            throw new AssertionError("Deleted key is still visible");
        }

        if (second.snapshot().containsKey("name")) {
            throw new AssertionError("Snapshot contains deleted key");
        }

        System.out.println("TEST 2 PASSED: Delete and snapshot");

        // TEST 3: Flush the tombstone and verify disk lookup.
        second.flush();

        if (second.get("name") != null) {
            throw new AssertionError(
                    "Deleted key reappeared after flush");
        }

        if (second.get("language") == null) {
            throw new AssertionError(
                    "Unrelated key disappeared after flush");
        }

        System.out.println("TEST 3 PASSED: Tombstone flush");

        // TEST 4: Simulate restart and verify deletion persists.
        StorageEngine third = new StorageEngine(testDir);

        if (third.get("name") != null) {
            throw new AssertionError(
                    "Deleted key reappeared after restart");
        }

        if (!"Java".equals(third.get("language"))) {
            throw new AssertionError(
                    "Language recovery failed after deletion");
        }

        if (third.snapshot().containsKey("name")) {
            throw new AssertionError(
                    "Restart snapshot contains deleted key");
        }

        System.out.println("Recovered snapshot: " + third.snapshot());
        System.out.println("Recovered SSTables: " + third.sstableCount());

        System.out.println("TEST 4 PASSED: Tombstone restart recovery");
        System.out.println("ALL STORAGE ENGINE TESTS PASSED");
    }
}
