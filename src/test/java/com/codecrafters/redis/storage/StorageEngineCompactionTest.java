
package com.codecrafters.redis.storage;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

public class StorageEngineCompactionTest {

    public static void main(String[] args) throws Exception {
        Path testDir = Path.of(
                "test-data", "compaction-recovery");

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

        // Create multiple SSTables and a deletion.
        try (StorageEngine engine = new StorageEngine(testDir)) {
            engine.put("name", "Mursalim");
            engine.put("language", "Java");
            engine.flush();

            engine.put("language", "Java 21");
            engine.flush();

            engine.delete("name");
            engine.flush();

            if (engine.sstableCount() < 2) {
                throw new AssertionError(
                        "Expected multiple SSTables before compaction");
            }

            engine.compact();

            if (engine.sstableCount() != 1) {
                throw new AssertionError(
                        "Expected one SSTable after compaction");
            }

            if (!"Java 21".equals(engine.get("language"))) {
                throw new AssertionError(
                        "Latest value missing after compaction");
            }

            if (engine.get("name") != null) {
                throw new AssertionError(
                        "Deleted key visible after compaction");
            }
        }

        // Reopen the engine to verify persisted state.
        try (StorageEngine recovered = new StorageEngine(testDir)) {
            if (!"Java 21".equals(recovered.get("language"))) {
                throw new AssertionError(
                        "Latest value missing after restart");
            }

            if (recovered.get("name") != null) {
                throw new AssertionError(
                        "Deleted key reappeared after restart");
            }

            if (recovered.sstableCount() != 1) {
                throw new AssertionError(
                        "Expected one SSTable after restart");
            }

            System.out.println(
                    "Recovered snapshot: " + recovered.snapshot());
        }

        System.out.println(
                "TEST PASSED: Compaction restart recovery");
    }

    @Test
    void runsExistingScenarioUnderJUnit() throws Exception {
        main(new String[0]);
    }
}
