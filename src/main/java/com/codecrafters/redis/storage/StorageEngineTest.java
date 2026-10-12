package com.codecrafters.redis.storage;

import java.nio.file.Files;
import java.nio.file.Path;

public class StorageEngineTest {
    public static void main(String[] args) throws Exception {
        Path testDir = Path.of("test-data", "restart-recovery");

        if (Files.exists(testDir)) {
            try (var paths = Files.walk(testDir)) {
                paths.sorted(java.util.Comparator.reverseOrder())
                     .forEach(path -> {
                         try {
                             Files.delete(path);
                         } catch (Exception e) {
                             throw new RuntimeException(e);
                         }
                     });
            }
        }

        // First instance: write and flush data to disk.
        StorageEngine first = new StorageEngine(testDir);
        first.put("name", "Mursalim");
        first.put("language", "Java");
        first.flush();

        System.out.println("First instance: " + first.snapshot());

        // Simulate restart by creating a new instance.
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

        System.out.println("ALL RESTART RECOVERY TESTS PASSED");
    }
}