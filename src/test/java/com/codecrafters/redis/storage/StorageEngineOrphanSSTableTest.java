
package com.codecrafters.redis.storage;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import java.util.stream.Stream;

public class StorageEngineOrphanSSTableTest {

    public static void main(String[] args) throws Exception {
        Path testDir = Files.createTempDirectory("orphan-sstable-recovery-");
        try {
            // Persist the original value.
            try (StorageEngine engine = new StorageEngine(testDir)) {
                engine.put("language", "Java");
                engine.flush();
            }

            // Simulate a crash after the SSTable was written but before its
            // manifest was published. The next flush reuses this generation.
            Path orphan = testDir.resolve(
                    "sstable-00000000000000000002.sst");
            SSTable.write(orphan, Map.of(
                    "language", "Incorrect",
                    "orphan", "unexpected"));

            // Recover, write new data, and flush it over the orphan path.
            try (StorageEngine recovered = new StorageEngine(testDir)) {
                if (!"Java".equals(recovered.get("language"))) {
                    throw new AssertionError("Active value was overwritten");
                }

                recovered.put("database", "DistributedKVStore");
                recovered.flush();
            }

            // Verify the latest data survives another restart, while the
            // overwritten orphan's records never become visible.
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
        } finally {
            deleteTree(testDir);
        }
    }

    private static void deleteTree(Path root) throws Exception {
        if (!Files.exists(root)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(root)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }
}
