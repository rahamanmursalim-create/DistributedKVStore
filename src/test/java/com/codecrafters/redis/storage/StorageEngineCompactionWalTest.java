package com.codecrafters.redis.storage;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

public class StorageEngineCompactionWalTest {

    public static void main(String[] args) throws Exception {
        Path testDir = Path.of("test-data", "compaction-wal-recovery");

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

        // First create multiple SSTables.
        try (StorageEngine engine = new StorageEngine(testDir)) {
            engine.put("old", "persisted");
            engine.flush();

            engine.put("another", "persisted-too");
            engine.flush();

            // Keep this write in the MemTable and WAL.
            engine.put("new", "unflushed");

            // Compact the existing SSTables without flushing the MemTable.
            engine.compact();

            if (!"unflushed".equals(engine.get("new"))) {
                throw new AssertionError(
                        "Unflushed value missing before restart");
            }
        }

        // Restart and verify the unflushed write survived.
        try (StorageEngine recovered = new StorageEngine(testDir)) {
            if (!"unflushed".equals(recovered.get("new"))) {
                throw new AssertionError(
                        "Unflushed value lost after compaction and restart");
            }

            if (!"persisted".equals(recovered.get("old"))) {
                throw new AssertionError(
                        "Previously persisted value missing after restart");
            }

            if (!"persisted-too".equals(recovered.get("another"))) {
                throw new AssertionError(
                        "Second persisted value missing after restart");
            }

            System.out.println(
                    "Recovered snapshot: " + recovered.snapshot());
        }

        System.out.println(
                "TEST PASSED: Compaction preserves unflushed WAL data");
    }

    @Test
    void runsExistingScenarioUnderJUnit() throws Exception {
        main(new String[0]);
    }
}