package com.codecrafters.redis.raft;

import org.junit.jupiter.api.Test;

import com.codecrafters.redis.storage.RedisStore;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Seeded randomized histories checked against a single-register linearizability model. */
public final class RaftLinearizabilityTest {
    private record Op(long start, long end, String kind, String argument, String result) { }

    public static void main(String[] args) throws Exception {
        long seed = Long.getLong("kv.test.historySeed", 0x5eedc0deL);
        Path dir = Files.createTempDirectory("raft-linearizability");
        int port;
        try (var socket = new java.net.ServerSocket(0)) { port = socket.getLocalPort(); }
        try (RedisStore store = new RedisStore(dir.resolve("store.wal"), false);
             RaftGroup group = new RaftGroup("history", "n1", new InetSocketAddress("127.0.0.1", port), Map.of(), dir, store)) {
            group.start();
            long electionDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
            while (!group.isLeader() && System.nanoTime() < electionDeadline) Thread.sleep(10);
            if (!group.isLeader()) throw new AssertionError("Single-node Raft did not elect a leader");

            int workers = 4, perWorker = 12;
            CountDownLatch ready = new CountDownLatch(workers), start = new CountDownLatch(1);
            List<Op> history = java.util.Collections.synchronizedList(new ArrayList<>());
            AtomicReference<Throwable> failure = new AtomicReference<>();
            List<Thread> threads = new ArrayList<>();
            for (int w = 0; w < workers; w++) {
                final int worker = w;
                Thread thread = new Thread(() -> {
                    Random random = new Random(seed ^ (worker * 0x9e3779b9L));
                    ready.countDown();
                    try {
                        start.await();
                        for (int i = 0; i < perWorker; i++) {
                            boolean write = random.nextInt(100) < 65;
                            String value = "v" + worker + "-" + i;
                            long began = System.nanoTime();
                            if (write) group.put("register", value.getBytes(StandardCharsets.UTF_8));
                            else {
                                byte[] read = group.getWithReadIndex("register");
                                value = read == null ? null : new String(read, StandardCharsets.UTF_8);
                            }
                            long ended = System.nanoTime();
                            history.add(new Op(began, ended, write ? "put" : "get", write ? value : "", value));
                        }
                    } catch (Throwable t) { failure.compareAndSet(null, t); }
                }, "history-worker-" + w);
                threads.add(thread); thread.start();
            }
            if (!ready.await(5, TimeUnit.SECONDS)) throw new AssertionError("History workers did not initialize");
            start.countDown();
            for (Thread thread : threads) thread.join(30_000);
            for (Thread thread : threads) if (thread.isAlive()) throw new AssertionError("History worker stalled");
            if (failure.get() != null) throw new AssertionError("History operation failed", failure.get());
            if (!linearizable(history)) throw new AssertionError("Non-linearizable history, seed=" + seed + ", operations=" + history);

            // Deterministic randomized sequential reference-model check adds 1,000 command transitions.
            Random modelRandom = new Random(seed ^ 0x51a7eL);
            String model = null;
            for (int i = 0; i < 1000; i++) {
                if (modelRandom.nextBoolean()) {
                    model = "model-" + modelRandom.nextInt(10000);
                    group.put("model", model.getBytes(StandardCharsets.UTF_8));
                } else {
                    byte[] actual = group.getWithReadIndex("model");
                    String value = actual == null ? null : new String(actual, StandardCharsets.UTF_8);
                    if (!java.util.Objects.equals(model, value)) throw new AssertionError("Seeded model mismatch at step " + i + ", seed=" + seed);
                }
            }
        } finally {
            try (var paths = Files.walk(dir)) { paths.sorted(java.util.Comparator.reverseOrder()).forEach(path -> { try { Files.deleteIfExists(path); } catch (Exception e) { throw new RuntimeException(e); } }); }
        }
        System.out.println("RaftLinearizabilityTest passed; seed=" + seed + ", concurrentOps=48, modelOps=1000");
    }

    private static boolean linearizable(List<Op> operations) {
        List<Op> ops = new ArrayList<>(operations);
        return search(ops, new boolean[ops.size()], null, 0);
    }

    private static boolean search(List<Op> ops, boolean[] used, String value, int count) {
        if (count == ops.size()) return true;
        for (int i = 0; i < ops.size(); i++) {
            if (used[i]) continue;
            Op candidate = ops.get(i);
            boolean hasPredecessor = false;
            for (int j = 0; j < ops.size(); j++)
                if (!used[j] && j != i && ops.get(j).end() < candidate.start()) { hasPredecessor = true; break; }
            if (hasPredecessor) continue;
            String next = value;
            if (candidate.kind().equals("put")) next = candidate.argument();
            else if (!java.util.Objects.equals(value, candidate.result())) continue;
            used[i] = true;
            if (search(ops, used, next, count + 1)) return true;
            used[i] = false;
        }
        return false;
    }

    @Test
    void runsExistingScenarioUnderJUnit() throws Exception {
        main(new String[0]);
    }
}
