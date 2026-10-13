package com.codecrafters.redis.storage;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

public class StorageEngineWalRecoveryTest {

    public static void main(String[] args) throws Exception {
        Path dir = Path.of("test-data", "wal-recovery");

        if (Files.exists(dir)) {
            try (var paths = Files.walk(dir)) {
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

        // Write data without flushing the MemTable.
        StorageEngine first = new StorageEngine(dir);
        first.put("name", "Mursalim");
        first.put("language", "Java");
        first.close();

        // Restart: unflushed data must be recovered from the WAL.
        StorageEngine second = new StorageEngine(dir);

        check("Mursalim".equals(second.get("name")),
                "Unflushed PUT recovery failed");
        check("Java".equals(second.get("language")),
                "Second unflushed PUT recovery failed");

        System.out.println("TEST 1 PASSED: Unflushed PUT recovery");

        // Delete must also survive a restart without a flush.
        second.delete("name");
        second.close();

        StorageEngine third = new StorageEngine(dir);

        check(third.get("name") == null,
                "Deleted key reappeared after WAL recovery");
        check("Java".equals(third.get("language")),
                "Unrelated key was lost after WAL recovery");

        System.out.println("TEST 2 PASSED: Unflushed DELETE recovery");

        third.close();
        System.out.println("ALL STORAGE ENGINE WAL RECOVERY TESTS PASSED");
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}