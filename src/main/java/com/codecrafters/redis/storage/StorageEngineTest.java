package com.codecrafters.redis.storage;

import java.nio.file.Files;
import java.nio.file.Path;

public class StorageEngineTest {
    public static void main(String[] args) throws Exception {
        Path testDir = Path.of("test-data", "storage-engine");

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

        StorageEngine engine = new StorageEngine(testDir);

        engine.put("name", "Mursalim");
        engine.put("language", "Java");
        engine.flush();

        engine.put("name", "UpdatedName");
        engine.put("city", "Kolkata");
        engine.flush();

        System.out.println("Name: " + engine.get("name"));
        System.out.println("Language: " + engine.get("language"));
        System.out.println("City: " + engine.get("city"));
        System.out.println("SSTable count: " + engine.sstableCount());
        System.out.println("Snapshot: " + engine.snapshot());

        if (!"UpdatedName".equals(engine.get("name"))) {
            throw new AssertionError("Overwritten key test failed");
        }

        if (!"Java".equals(engine.get("language"))) {
            throw new AssertionError("Older SSTable lookup failed");
        }

        if (!"Kolkata".equals(engine.get("city"))) {
            throw new AssertionError("Second SSTable lookup failed");
        }

        if (engine.sstableCount() != 2) {
            throw new AssertionError("Expected 2 SSTables");
        }

        System.out.println("ALL MULTI-SSTABLE TESTS PASSED");
    }
}