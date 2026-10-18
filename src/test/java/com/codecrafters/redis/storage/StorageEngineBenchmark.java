
package com.codecrafters.redis.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

public final class StorageEngineBenchmark {

    private static final int KEY_COUNT = 2_000;
    private static final int READ_COUNT = 5_000;
    private static final int WARMUP_COUNT = 500;
    private static final int PROGRESS_INTERVAL = 100;
    private static final int UPDATE_COUNT = 500;

    private StorageEngineBenchmark() {
    }

    public static void main(String[] args) throws Exception {
        Path directory =
                Files.createTempDirectory("lsm-benchmark-");

        try {
            runBenchmark(directory);
        } finally {
            deleteDirectory(directory);
        }
    }

    private static void runBenchmark(Path directory)
            throws IOException {

        try (StorageEngine engine = new StorageEngine(directory)) {

            // Phase 1: Write the initial data.
            System.out.println("Phase 1: Writing "
                    + KEY_COUNT + " keys...");

            long writeStart = System.nanoTime();

            for (int i = 0; i < KEY_COUNT; i++) {
                engine.put("key:" + i, "value:" + i);

                if ((i + 1) % PROGRESS_INTERVAL == 0) {
                    System.out.println(
                            "Written: " + (i + 1) + "/" + KEY_COUNT);
                }
            }

            printDuration(
                    "Initial write",
                    System.nanoTime() - writeStart);

            // Phase 2: Flush initial data to an SSTable.
            System.out.println(
                    "Phase 2: Flushing initial SSTable...");

            long flushStart = System.nanoTime();
            engine.flush();

            printDuration(
                    "Initial flush",
                    System.nanoTime() - flushStart);

            // Phase 3: Update some existing keys.
            int updateCount = Math.min(UPDATE_COUNT, KEY_COUNT);

            System.out.println("Phase 3: Updating "
                    + updateCount + " keys...");

            long updateStart = System.nanoTime();

            for (int i = 0; i < updateCount; i++) {
                engine.put("key:" + i, "updated:" + i);
            }

            printDuration(
                    "Update writes",
                    System.nanoTime() - updateStart);

            // Phase 4: Flush updates to a second SSTable.
            System.out.println(
                    "Phase 4: Flushing updated SSTable...");

            flushStart = System.nanoTime();
            engine.flush();

            printDuration(
                    "Update flush",
                    System.nanoTime() - flushStart);

            System.out.println("Active SSTables: "
                    + engine.sstableCount());

            // Prepare a repeatable randomized key order.
            List<String> keys = new ArrayList<>(KEY_COUNT);

            for (int i = 0; i < KEY_COUNT; i++) {
                keys.add("key:" + i);
            }

            Collections.shuffle(keys, new Random(42));

            // Phase 5: Warm up GET operations.
            System.out.println("Phase 5: Warming up GET...");

            long warmupStart = System.nanoTime();

            for (int i = 0; i < WARMUP_COUNT; i++) {
                String key = keys.get(i % keys.size());
                String value = engine.get(key);

                if (value == null) {
                    throw new IllegalStateException(
                            "Unexpected missing key during warm-up: "
                                    + key);
                }
            }

            printDuration(
                    "GET warm-up",
                    System.nanoTime() - warmupStart);

            // Phase 6: Benchmark GET operations.
            System.out.println("Phase 6: Benchmarking "
                    + READ_COUNT + " GET operations...");

            long readStart = System.nanoTime();
            int successfulReads = 0;

            for (int i = 0; i < READ_COUNT; i++) {
                String key = keys.get(i % keys.size());
                String value = engine.get(key);

                if (value != null) {
                    successfulReads++;
                }
            }

            long elapsed = System.nanoTime() - readStart;

            double elapsedMs = elapsed / 1_000_000.0;
            double elapsedSeconds = elapsed / 1_000_000_000.0;
            double averageMicros =
                    elapsed / 1_000.0 / READ_COUNT;
            double operationsPerSecond =
                    READ_COUNT / elapsedSeconds;

            // Print benchmark results.
            System.out.println();
            System.out.println(
                    "===== StorageEngine GET Benchmark =====");
            System.out.println("Keys: " + KEY_COUNT);
            System.out.println("Active SSTables: "
                    + engine.sstableCount());
            System.out.println("Read operations: " + READ_COUNT);
            System.out.println("Successful reads: "
                    + successfulReads);
            System.out.printf(
                    "Total time: %.2f ms%n", elapsedMs);
            System.out.printf(
                    "Average latency: %.2f microseconds%n",
                    averageMicros);
            System.out.printf(
                    "Throughput: %.0f GETs/second%n",
                    operationsPerSecond);
            System.out.println(
                    "=======================================");

            // Print SSTable lookup statistics.
            System.out.println();
            System.out.println(
                    "===== SSTable Lookup Statistics =====");

            System.out.print(engine.getSSTableLookupStats());

            System.out.println(
                    "======================================");

            // Warn if any reads failed.
            if (successfulReads != READ_COUNT) {
                System.out.println(
                        "WARNING: Some GET operations failed.");
            }
        }
    }

    private static void printDuration(
            String label,
            long nanos) {

        System.out.printf(
                "%s completed in %.2f seconds%n",
                label,
                nanos / 1_000_000_000.0);
    }

    private static void deleteDirectory(Path directory)
            throws IOException {

        try (var paths = Files.walk(directory)) {
            for (Path path : paths
                    .sorted(Comparator.reverseOrder())
                    .toList()) {

                Files.deleteIfExists(path);
            }
        }
    }
}
