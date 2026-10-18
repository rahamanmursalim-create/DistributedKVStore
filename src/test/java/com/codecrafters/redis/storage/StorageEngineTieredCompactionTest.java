package com.codecrafters.redis.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;

public class StorageEngineTieredCompactionTest {

    private static final Path TEST_DIR = Path.of("test-data", "tiered-compaction-test");

    public static void main(String[] args) throws Exception {
        cleanDirectory(TEST_DIR);
        Files.createDirectories(TEST_DIR);

        try {
            testCompactBatch();
            cleanDirectory(TEST_DIR);
            Files.createDirectories(TEST_DIR);

            testSizeTieredCompaction();
            cleanDirectory(TEST_DIR);
            Files.createDirectories(TEST_DIR);

            testRecoveryAfterBatchCompaction();
        } finally {
            cleanDirectory(TEST_DIR);
        }

        System.out.println("StorageEngineTieredCompactionTest passed all tests.");
    }

    private static void testCompactBatch() throws Exception {
        try (StorageEngine engine = new StorageEngine(TEST_DIR)) {
            // Table 1
            engine.put("k1", "v1");
            engine.put("k2", "v2");
            engine.flush();

            // Table 2
            engine.put("k2", "v2_updated");
            engine.put("k3", "v3");
            engine.flush();

            // Table 3
            engine.put("k4", "v4");
            engine.put("k5", "v5");
            engine.flush();

            // Table 4
            engine.put("k4", "v4_final");
            engine.delete("k1");
            engine.flush();

            assert engine.activeTableCount() == 4 : "Expected 4 active SSTables, found " + engine.activeTableCount();

            // Compact the 2 newest tables (Tables 3 and 4)
            engine.compactBatch(2);

            assert engine.activeTableCount() == 3 : "Expected 3 active SSTables after compactBatch(2), found " + engine.activeTableCount();

            // Check correctness of data across the remaining and compacted tables
            assert engine.get("k1") == null : "k1 should be deleted (tombstone preserved)";
            assert "v2_updated".equals(engine.get("k2")) : "k2 should be v2_updated";
            assert "v3".equals(engine.get("k3")) : "k3 should be v3";
            assert "v4_final".equals(engine.get("k4")) : "k4 should be v4_final";
            assert "v5".equals(engine.get("k5")) : "k5 should be v5";
        }
    }

    private static void testSizeTieredCompaction() throws Exception {
        try (StorageEngine engine = new StorageEngine(TEST_DIR)) {
            for (int i = 1; i <= 4; i++) {
                engine.put("batch" + i, "data" + i);
                engine.flush();
            }

            assert engine.activeTableCount() == 4 : "Expected 4 SSTables";

            // Threshold is 5: should not trigger compaction
            boolean triggered = engine.sizeTieredCompact(5, 2);
            assert !triggered : "Should not trigger when below threshold";
            assert engine.activeTableCount() == 4;

            // Threshold is 4: should compact 2 tables
            triggered = engine.sizeTieredCompact(4, 2);
            assert triggered : "Should trigger when at threshold";
            assert engine.activeTableCount() == 3 : "Active table count should be reduced to 3";

            for (int i = 1; i <= 4; i++) {
                assert ("data" + i).equals(engine.get("batch" + i));
            }
        }
    }

    private static void testRecoveryAfterBatchCompaction() throws Exception {
        try (StorageEngine engine = new StorageEngine(TEST_DIR)) {
            engine.put("rec1", "val1");
            engine.flush();

            engine.put("rec2", "val2");
            engine.flush();

            engine.put("rec3", "val3");
            engine.flush();

            engine.compactBatch(2);
            assert engine.activeTableCount() == 2;
        }

        // Reopen storage engine from same directory to verify MANIFEST and recovery
        try (StorageEngine reopened = new StorageEngine(TEST_DIR)) {
            assert reopened.activeTableCount() == 2 : "Reopened should have 2 active SSTables";
            assert "val1".equals(reopened.get("rec1"));
            assert "val2".equals(reopened.get("rec2"));
            assert "val3".equals(reopened.get("rec3"));
        }
    }

    private static void cleanDirectory(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }

        List<Path> paths;
        try (var stream = Files.walk(dir)) {
            paths = stream.sorted(Comparator.reverseOrder()).toList();
        }

        for (Path path : paths) {
            Files.deleteIfExists(path);
        }
    }
}
