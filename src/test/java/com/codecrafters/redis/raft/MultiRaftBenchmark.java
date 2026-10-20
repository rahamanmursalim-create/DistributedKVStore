package com.codecrafters.redis.raft;

import com.codecrafters.redis.storage.RedisStore;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Reproducible local Multi-Raft mixed-workload microbenchmark; results are not SLO claims. */
public final class MultiRaftBenchmark {
    private static final int GROUPS = 2;
    private static final int REPLICAS = 3;
    private static final int WORKERS = 4;
    private static final int OPERATIONS_PER_WORKER = 200;

    public static void main(String[] args) throws Exception {
        Path root = Files.createTempDirectory("multi-raft-benchmark");
        int[][] ports = new int[GROUPS][REPLICAS];
        Set<Integer> usedPorts = new HashSet<>();
        RaftGroup[][] nodes = new RaftGroup[GROUPS][REPLICAS];
        RedisStore[][] stores = new RedisStore[GROUPS][REPLICAS];
        MultiRaftStore router = null;
        try {
            for (int group = 0; group < GROUPS; group++)
                for (int node = 0; node < REPLICAS; node++) ports[group][node] = availablePort(usedPorts);
            String[] ids = {"n1", "n2", "n3"};
            String[] groupIds = {"g1", "g2"};
            for (int group = 0; group < GROUPS; group++) {
                for (int node = 0; node < REPLICAS; node++) {
                    Map<String, InetSocketAddress> peers = new LinkedHashMap<>();
                    for (int peer = 0; peer < REPLICAS; peer++) if (peer != node)
                        peers.put(ids[peer], new InetSocketAddress("127.0.0.1", ports[group][peer]));
                    Path dir = root.resolve(groupIds[group] + "-" + ids[node]);
                    stores[group][node] = new RedisStore(dir.resolve("state.wal"), false);
                    nodes[group][node] = new RaftGroup(groupIds[group], ids[node],
                            new InetSocketAddress("127.0.0.1", ports[group][node]), peers, dir, stores[group][node]);
                }
            }
            for (RaftGroup[] group : nodes) for (RaftGroup node : group) node.start();
            Map<String, RaftGroup> leaders = awaitLeaders(nodes, groupIds);
            router = new MultiRaftStore(leaders, 128, false);
            Map<String, String> initial = new LinkedHashMap<>();
            for (int i = 0; i < 512; i++) initial.put("bench-key-" + i, "seed-" + i);
            router.setBatch(initial);
            System.out.println("Benchmark path: MultiRaftStore API (not TCP/RESP); writes use synchronous WAL and quorum replication.");
            runWorkload(router, 50, "warmup");
            for (int run = 1; run <= 3; run++) runWorkload(router, OPERATIONS_PER_WORKER, "run-" + run);
        } finally {
            if (router != null) try { router.close(); } catch (RuntimeException ignored) { }
            for (RaftGroup[] group : nodes) for (RaftGroup node : group)
                if (node != null) try { node.close(); } catch (Exception ignored) { }
            for (RedisStore[] group : stores) for (RedisStore store : group)
                if (store != null) store.close();
            deleteTree(root);
        }
    }

    private static void runWorkload(MultiRaftStore router, int operationsPerWorker, String label) throws Exception {
        int count = WORKERS * operationsPerWorker;
        long[] latencyNanos = new long[count];
        CountDownLatch ready = new CountDownLatch(WORKERS);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(WORKERS);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        List<Thread> workers = new ArrayList<>();
        for (int worker = 0; worker < WORKERS; worker++) {
            final int workerId = worker;
            Thread thread = new Thread(() -> {
                ready.countDown();
                try {
                    start.await();
                    for (int i = 0; i < operationsPerWorker; i++) {
                        int sequence = workerId * operationsPerWorker + i;
                        String key = "bench-key-" + ((sequence * 31) % 512);
                        long before = System.nanoTime();
                        if (sequence % 2 == 0) router.get(key);
                        else router.set("bench-write-" + sequence, "v-" + sequence);
                        latencyNanos[sequence] = System.nanoTime() - before;
                    }
                } catch (Throwable t) { failure.compareAndSet(null, t); }
                finally { done.countDown(); }
            }, "multi-raft-benchmark-" + worker);
            workers.add(thread);
            thread.start();
        }
        if (!ready.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("Workers failed to initialize");
        long startNanos = System.nanoTime();
        start.countDown();
        if (!done.await(3, TimeUnit.MINUTES)) throw new IllegalStateException("Benchmark timed out");
        long elapsedNanos = System.nanoTime() - startNanos;
        if (failure.get() != null) throw new IllegalStateException("Benchmark operation failed", failure.get());
        if ("warmup".equals(label)) {
            System.out.printf("Warmup complete: operations=%d, durationSec=%.3f%n", count,
                    elapsedNanos / 1_000_000_000.0);
            return;
        }
        java.util.Arrays.sort(latencyNanos);
        System.out.printf("Environment: OS=%s %s, CPUs=%d, JVM=%s, maxHeapMiB=%d%n",
                System.getProperty("os.name"), System.getProperty("os.version"),
                Runtime.getRuntime().availableProcessors(), System.getProperty("java.version"),
                Runtime.getRuntime().maxMemory() / (1024 * 1024));
        if (label.equals("run-1")) {
            System.out.printf("Topology: groups=%d, replicasPerGroup=%d, workers=%d, local nodes share one host%n",
                    GROUPS, REPLICAS, WORKERS);
            System.out.printf("Workload: reads=50%%, writes=50%%, dataset=512 seeded keys, value sizes ~10 bytes%n");
        }
        System.out.printf("%s: operations=%d, elapsedSec=%.3f, throughputOpsSec=%.1f, p50Ms=%.3f, p95Ms=%.3f, p99Ms=%.3f, errors=0%n",
                label, count, elapsedNanos / 1_000_000_000.0, count / (elapsedNanos / 1_000_000_000.0),
                percentile(latencyNanos, .50) / 1_000_000.0,
                percentile(latencyNanos, .95) / 1_000_000.0,
                percentile(latencyNanos, .99) / 1_000_000.0);
    }

    private static long percentile(long[] sorted, double percentile) {
        return sorted[Math.min(sorted.length - 1, (int) Math.ceil(percentile * sorted.length) - 1)];
    }

    private static Map<String, RaftGroup> awaitLeaders(RaftGroup[][] nodes, String[] groupIds) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        Map<String, RaftGroup> leaders = new LinkedHashMap<>();
        while (System.nanoTime() < deadline && leaders.size() < nodes.length) {
            for (int group = 0; group < nodes.length; group++)
                for (RaftGroup node : nodes[group]) if (node.isLeader()) leaders.put(groupIds[group], node);
            Thread.sleep(20);
        }
        if (leaders.size() != nodes.length) throw new IllegalStateException("Raft leaders were not elected");
        return leaders;
    }

    private static int availablePort(Set<Integer> used) throws Exception {
        while (true) try (ServerSocket socket = new ServerSocket(0)) {
            int port = socket.getLocalPort();
            if (used.add(port)) return port;
        }
    }

    private static void deleteTree(Path root) throws Exception {
        try (var paths = Files.walk(root)) {
            for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) Files.deleteIfExists(path);
        }
    }
}
