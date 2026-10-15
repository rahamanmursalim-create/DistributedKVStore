package com.codecrafters.redis.storage;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;

public class SSTableBenchmark {

    public static void main(String[] args) throws Exception {
        int[] sizes = {1_000, 10_000, 50_000};
        Path dir = Path.of("test-data", "sstable-benchmark");
        Files.createDirectories(dir);

        for (int size : sizes) {
            Map<String, String> data = new TreeMap<>();

            for (int i = 0; i < size; i++) {
                data.put(String.format("key-%08d", i), "value-" + i);
            }

            String[] keys = new String[size];

            for (int i = 0; i < size; i++) {
                keys[i] = String.format("key-%08d", i);
            }

            Path file = dir.resolve("benchmark-" + size + ".sst");
            SSTable table = SSTable.write(file, data);

            // Initialize the sparse index and Bloom filter.
            long initStart = System.nanoTime();
            table.get(keys[size / 2]);
            long initNanos = System.nanoTime() - initStart;

            int warmup = 500;
            int lookups = 5_000;

            // Benchmark 1: Repeated lookups of one cached key.
            String hotKey = keys[size / 2];

            for (int i = 0; i < warmup; i++) {
                table.get(hotKey);
            }

            long hotStart = System.nanoTime();

            for (int i = 0; i < lookups; i++) {
                if (table.get(hotKey) == null) {
                    throw new AssertionError("Missing key: " + hotKey);
                }
            }

            long hotNanos = System.nanoTime() - hotStart;

            // Benchmark 2: Lookups across a larger working set.
            int workingSet = Math.min(size, 10_000);

            for (int i = 0; i < warmup; i++) {
                table.get(keys[(i * 31) % workingSet]);
            }

            long mixedStart = System.nanoTime();

            for (int i = 0; i < lookups; i++) {
                String key = keys[(i * 31) % workingSet];

                if (table.get(key) == null) {
                    throw new AssertionError("Missing key: " + key);
                }
            }

            long mixedNanos = System.nanoTime() - mixedStart;

            System.out.printf(
                "Keys: %,d | Initialization: %.2f ms | "
                    + "Hot-key average: %.3f us | "
                    + "Working set (%d keys) average: %.3f us%n",
                size,
                initNanos / 1_000_000.0,
                hotNanos / (double) lookups / 1_000.0,
                workingSet,
                mixedNanos / (double) lookups / 1_000.0
            );
        }
    }
}
