package com.codecrafters.redis.raft;

import com.codecrafters.redis.storage.RedisStore;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/** Reproducible single-node durable Raft write benchmark; arguments: operation-count and value-bytes. */
public final class RaftBenchmark {
    public static void main(String[] args) throws Exception {
        int operations = args.length > 0 ? Integer.parseInt(args[0]) : 5_000;
        int valueBytes = args.length > 1 ? Integer.parseInt(args[1]) : 128;
        if (operations < 1 || operations > 100_000 || valueBytes < 1 || valueBytes > 1024 * 1024)
            throw new IllegalArgumentException("operations must be 1..100000 and value-bytes 1..1048576");
        Path root = Files.createTempDirectory("raft-benchmark");
        byte[] value = new byte[valueBytes]; java.util.Arrays.fill(value, (byte) 'x');
        long[] latency = new long[operations];
        try (RedisStore store = new RedisStore(root.resolve("state.wal"), false);
             RaftGroup group = new RaftGroup("bench", "n1", new InetSocketAddress("127.0.0.1", 0),
                     Map.of(), root.resolve("raft"), store)) {
            group.start(); if (!group.startElection()) throw new IllegalStateException("Single-node leader election failed");
            for (int i = 0; i < Math.min(200, operations); i++) group.put("warmup-" + i, value);
            long started = System.nanoTime();
            for (int i = 0; i < operations; i++) {
                long one = System.nanoTime(); group.put("key-" + i, value);
                latency[i] = System.nanoTime() - one;
            }
            long elapsed = System.nanoTime() - started;
            java.util.Arrays.sort(latency);
            double seconds = elapsed / 1_000_000_000.0;
            System.out.printf("jdk=%s os=%s arch=%s ops=%d value_bytes=%d elapsed_s=%.3f throughput_ops_s=%.1f p50_ms=%.3f p95_ms=%.3f p99_ms=%.3f%n",
                    System.getProperty("java.version"), System.getProperty("os.name"), System.getProperty("os.arch"),
                    operations, valueBytes, seconds, operations / seconds,
                    millis(latency[(operations - 1) / 2]), millis(latency[(int) Math.ceil(operations * .95) - 1]),
                    millis(latency[(int) Math.ceil(operations * .99) - 1]));
        } finally {
            try (var paths = Files.walk(root)) {
                paths.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
                    try { Files.deleteIfExists(path); } catch (Exception e) { throw new RuntimeException(e); }
                });
            }
        }
    }
    private static double millis(long nanos) { return nanos / 1_000_000.0; }
}
