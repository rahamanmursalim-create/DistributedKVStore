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

            // First lookup builds the index and Bloom Filter.
            long initStart = System.nanoTime();
            table.get(keys[size / 2]);
            long initNanos = System.nanoTime() - initStart;

            int warmup = 500;
            int lookups = 5_000;

            for (int i = 0; i < warmup; i++) {
                int keyNumber = (i * 31) % size;
                table.get(keys[keyNumber]);
            }

            long[] latencies = new long[lookups];
            long lookupStart = System.nanoTime();

            for (int i = 0; i < lookups; i++) {
                int keyNumber = (i * 31) % size;
                String key = keys[keyNumber];

                long start = System.nanoTime();
                String value = table.get(key);
                latencies[i] = System.nanoTime() - start;

                if (value == null) {
                    throw new AssertionError("Missing key: " + key);
                }
            }

            long lookupNanos = System.nanoTime() - lookupStart;
            java.util.Arrays.sort(latencies);

            System.out.printf(
                "Keys: %,d | Index build + first lookup: %.2f ms | "
                + "Average: %.3f us | Median: %.3f us | "
                + "Throughput: %.0f lookups/sec%n",
                size,
                initNanos / 1_000_000.0,
                lookupNanos / (double) lookups / 1_000.0,
                latencies[lookups / 2] / 1_000.0,
                lookups / (lookupNanos / 1_000_000_000.0)
            );
        }
    }
}
