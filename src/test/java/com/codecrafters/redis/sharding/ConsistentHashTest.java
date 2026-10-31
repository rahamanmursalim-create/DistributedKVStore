package com.codecrafters.redis.sharding;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public final class ConsistentHashTest {
    public static void main(String[] args) throws Exception {
        ConsistentHash<String> ring = new ConsistentHash<>(128);
        assert ring.getNode("key") == null;
        ring.addNode("node-a");
        Map<String, String> initialOwners = owners(ring, 10_000);
        ring.addNode("node-b");
        Map<String, String> afterAdd = owners(ring, 10_000);
        long changed = initialOwners.entrySet().stream()
                .filter(entry -> !entry.getValue().equals(afterAdd.get(entry.getKey())))
                .count();
        assert changed > 0 && changed < 10_000 : "adding a node should remap only a subset of keys";

        List<String> replicas = ring.getNodes("replicated-key", 10);
        assert replicas.size() == 2 : replicas;
        assert replicas.get(0).equals(ring.getNode("replicated-key"));
        assert ring.getNodes("replicated-key", 1).size() == 1;

        ring.removeNode("node-b");
        assert ring.size() == 1;
        for (int i = 0; i < 100; i++) assert ring.getNode("key-" + i).equals("node-a");

        ring.addNode("node-b");
        CountDownLatch start = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread[] readers = new Thread[8];
        for (int thread = 0; thread < readers.length; thread++) {
            int id = thread;
            readers[thread] = new Thread(() -> {
                try {
                    start.await();
                    for (int i = 0; i < 2_000; i++) {
                        if (ring.getNode("concurrent-" + id + '-' + i) == null) {
                            throw new AssertionError("ring returned no owner");
                        }
                        if (ring.getNodes("replica-" + i, 2).size() != 2) {
                            throw new AssertionError("ring returned incomplete replica set");
                        }
                    }
                } catch (Throwable t) { failure.compareAndSet(null, t); }
            });
            readers[thread].start();
        }
        start.countDown();
        for (Thread reader : readers) reader.join();
        if (failure.get() != null) throw new AssertionError("concurrent lookup failed", failure.get());
        System.out.println("ConsistentHashTest passed");
    }

    private static Map<String, String> owners(ConsistentHash<String> ring, int count) {
        Map<String, String> owners = new HashMap<>();
        for (int i = 0; i < count; i++) owners.put("key-" + i, ring.getNode("key-" + i));
        return owners;
    }

    @Test
    void runsExistingScenarioUnderJUnit() throws Exception {
        main(new String[0]);
    }
}
