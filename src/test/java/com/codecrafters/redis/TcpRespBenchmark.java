package com.codecrafters.redis;

import com.codecrafters.redis.storage.RedisStore;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/** End-to-end TCP/RESP benchmark for the standalone server; results are not SLO claims. */
public final class TcpRespBenchmark {
    private static final int CLIENTS = Integer.getInteger("bench.clients", 32);
    private static final int WARMUP_OPS_PER_CLIENT = Integer.getInteger("bench.warmup", 1000);
    private static final int OPS_PER_CLIENT = Integer.getInteger("bench.operations", 5000);

    public static void main(String[] args) throws Exception {
        Path root = Files.createTempDirectory("tcp-resp-benchmark");
        int port;
        try (ServerSocket reservation = new ServerSocket(0)) { port = reservation.getLocalPort(); }
        RedisStore store = new RedisStore(root.resolve("wal.log"), false);
        Server server = new Server(port, store);
        Thread service = new Thread(() -> {
            try { server.start(); } catch (Exception failure) { throw new RuntimeException(failure); }
        }, "tcp-resp-benchmark-server");
        service.setDaemon(true);
        service.start();
        try {
            awaitServer(port);
            System.out.printf("TCP/RESP benchmark: clients=%d, warmupOps=%d, measuredOpsPerRun=%d, workload=50%% GET / 50%% SET, valueBytes=64, batchMaxSize=%s, batchMaxDelayMillis=%s%n",
                    CLIENTS, CLIENTS * WARMUP_OPS_PER_CLIENT, CLIENTS * OPS_PER_CLIENT,
                    System.getProperty("redis.write.batch.maxSize", "128"),
                    System.getProperty("redis.write.batch.maxDelayMillis", "2"));
            run(port, WARMUP_OPS_PER_CLIENT, false, "warmup");
            for (int run = 1; run <= 3; run++) run(port, OPS_PER_CLIENT, true, "run-" + run);
        } finally {
            server.close();
            store.close();
            try (var paths = Files.walk(root)) {
                for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) Files.deleteIfExists(path);
            }
        }
    }

    private static void run(int port, int operationsPerClient, boolean measure, String label) throws Exception {
        int total = CLIENTS * operationsPerClient;
        long[][] latency = new long[CLIENTS][operationsPerClient];
        AtomicLong errors = new AtomicLong();
        AtomicReference<String> firstError = new AtomicReference<>();
        CountDownLatch ready = new CountDownLatch(CLIENTS), start = new CountDownLatch(1), done = new CountDownLatch(CLIENTS);
        List<Thread> workers = new ArrayList<>();
        for (int client = 0; client < CLIENTS; client++) {
            final int id = client;
            Thread worker = new Thread(() -> {
                try (Socket socket = new Socket("127.0.0.1", port);
                     BufferedInputStream in = new BufferedInputStream(socket.getInputStream());
                     BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream())) {
                    socket.setSoTimeout(10_000);
                    ready.countDown(); start.await();
                    for (int i = 0; i < operationsPerClient; i++) {
                        boolean write = (i & 1) == 0;
                        String command = write
                                ? resp("SET", "bench:" + (id * 1024 + ((i / 2) % 1024)), "v".repeat(64))
                                : resp("GET", "bench:" + (id * 1024 + ((i / 2) % 1024)));
                        long before = System.nanoTime();
                        try {
                            out.write(command.getBytes(StandardCharsets.UTF_8)); out.flush();
                            readReply(in);
                            if (measure) latency[id][i] = System.nanoTime() - before;
                        } catch (Exception failure) {
                            errors.incrementAndGet();
                            firstError.compareAndSet(null, failure.getClass().getSimpleName() + ": " + failure.getMessage());
                        }
                    }
                } catch (Exception failure) { errors.addAndGet(operationsPerClient); }
                finally { ready.countDown(); done.countDown(); }
            }, "tcp-resp-bench-client-" + client);
            workers.add(worker); worker.start();
        }
        if (!ready.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("Benchmark clients did not initialize");
        var osBean = ManagementFactory.getOperatingSystemMXBean();
        long cpuBefore = osBean instanceof com.sun.management.OperatingSystemMXBean bean
                ? bean.getProcessCpuTime() : -1;
        long startNanos = System.nanoTime(); start.countDown();
        if (!done.await(10, TimeUnit.MINUTES)) throw new IllegalStateException("Benchmark run timed out");
        long elapsedNanos = System.nanoTime() - startNanos;
        long cpuAfter = osBean instanceof com.sun.management.OperatingSystemMXBean bean
                ? bean.getProcessCpuTime() : -1;
        long cpuNanos = cpuBefore >= 0 && cpuAfter >= cpuBefore ? cpuAfter - cpuBefore : 0;
        if (!measure) {
            System.out.printf("Warmup complete: operations=%d, seconds=%.3f%n", total, elapsedNanos / 1e9);
            return;
        }
        long[] sorted = Arrays.stream(latency).flatMapToLong(Arrays::stream).filter(value -> value > 0).sorted().toArray();
        long failures = errors.get();
        System.out.printf("%s: ops=%d, elapsedSec=%.3f, throughputOpsSec=%.1f, p50Ms=%.3f, p95Ms=%.3f, p99Ms=%.3f, maxMs=%.3f, errors=%d, errorRate=%.6f%%, processCpuPercent=%.1f, usedHeapMiB=%d%n",
                label, total, elapsedNanos / 1e9, (total - failures) / (elapsedNanos / 1e9), percentile(sorted, .50) / 1e6,
                percentile(sorted, .95) / 1e6, percentile(sorted, .99) / 1e6, sorted[sorted.length - 1] / 1e6,
                failures, failures * 100.0 / total, cpuNanos * 100.0 / Math.max(1, elapsedNanos),
                (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024 * 1024));
        if (failures > 0) System.out.println("First error: " + firstError.get());
        long systemRamMiB = osBean instanceof com.sun.management.OperatingSystemMXBean bean
                ? bean.getTotalMemorySize() / (1024 * 1024) : -1;
        System.out.printf("Environment: OS=%s %s, JVM=%s, CPUs=%d, systemRamMiB=%d, maxHeapMiB=%d, GC=%s%n",
                System.getProperty("os.name"), System.getProperty("os.version"), System.getProperty("java.version"),
                Runtime.getRuntime().availableProcessors(), systemRamMiB,
                Runtime.getRuntime().maxMemory() / (1024 * 1024), gcStats());
    }

    private static String resp(String... args) {
        StringBuilder command = new StringBuilder("*").append(args.length).append("\r\n");
        for (String arg : args) command.append('$').append(arg.getBytes(StandardCharsets.UTF_8).length)
                .append("\r\n").append(arg).append("\r\n");
        return command.toString();
    }

    private static void readReply(BufferedInputStream in) throws Exception {
        int type = in.read(); if (type < 0) throw new java.io.EOFException();
        String line = readLine(in);
        if (type == '$') {
            int length = Integer.parseInt(line);
            if (length >= 0) { in.readNBytes(length); if (in.read() != '\r' || in.read() != '\n') throw new java.io.IOException("Bad bulk response"); }
        } else if (type != '+' && type != '-' && type != ':') throw new java.io.IOException("Unexpected RESP type: " + (char) type);
        else if (type == '-') throw new java.io.IOException("Server error: " + line);
    }

    private static String readLine(BufferedInputStream in) throws Exception {
        StringBuilder line = new StringBuilder(); int previous = -1;
        while (true) { int value = in.read(); if (value < 0) throw new java.io.EOFException();
            if (previous == '\r' && value == '\n') { line.setLength(line.length() - 1); return line.toString(); }
            line.append((char) value); previous = value; }
    }

    private static long percentile(long[] sorted, double p) {
        if (sorted.length == 0) return 0;
        return sorted[Math.min(sorted.length - 1, (int) Math.ceil(p * sorted.length) - 1)];
    }

    private static String gcStats() {
        return ManagementFactory.getGarbageCollectorMXBeans().stream()
                .map(bean -> bean.getName() + ":" + bean.getCollectionCount() + "/" + bean.getCollectionTime() + "ms")
                .toList().toString();
    }

    private static void awaitServer(int port) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) try (Socket ignored = new Socket("127.0.0.1", port)) { return; }
        catch (Exception unavailable) { Thread.sleep(20); }
        throw new IllegalStateException("TCP server did not start");
    }
}
