
package com.codecrafters.redis.storage;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;

public class StorageEngineOrphanSSTableTest {

    public static void main(String[] args) throws Exception {
        Path testDir = Path.of("test-data", "orphan-sstable-recovery");

        if (Files.exists(testDir)) {
            try (var paths = Files.walk(testDir)) {
                paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }

        // Persist the original value.
        try (StorageEngine engine = new StorageEngine(testDir)) {
            engine.put("language", "Java");
            engine.flush();
        }

        // Create an SSTable that is not referenced by MANIFEST.
        Path orphan = testDir.resolve(
                "sstable-00000000000000000002.sst");
        SSTable.write(orphan, Map.of(
                "language", "Incorrect",
                "orphan", "unexpected"));

        // Recover, write new data, and flush it.
        try (StorageEngine recovered = new StorageEngine(testDir)) {
            if (!"Java".equals(recovered.get("language"))) {
                throw new AssertionError("Active value was overwritten");
            }

            recovered.put("database", "DistributedKVStore");
            recovered.flush();
        }

        // Verify the latest data survives another restart.
        try (StorageEngine reopened = new StorageEngine(testDir)) {
            if (!"Java".equals(reopened.get("language"))) {
                throw new AssertionError("Original value was lost");
            }

            if (!"DistributedKVStore".equals(reopened.get("database"))) {
                throw new AssertionError("New value was lost");
            }

            if (reopened.get("orphan") != null) {
                throw new AssertionError("Orphan data was loaded");
            }
        }

        System.out.println(
                "TEST PASSED: Orphan SSTable recovery and subsequent flush");
    }
}
