
package com.codecrafters.redis.distributed;

import java.util.HashMap;
import java.util.Map;

public final class ConsistentHashRingTest {

    private ConsistentHashRingTest() {
    }

    public static void main(String[] args) {
        testEmptyRing();
        testNodeAddition();
        testKeyRouting();
        testDuplicateNode();
        testNodeRemoval();
        testVirtualNodes();
        testDistribution();

        System.out.println(
                "All ConsistentHashRing tests passed.");
    }

    private static void testEmptyRing() {
        ConsistentHashRing ring = new ConsistentHashRing(100);

        expectThrows(
                IllegalStateException.class,
                () -> ring.getNode("key:1"));

        System.out.println("PASS: Empty ring");
    }

    private static void testNodeAddition() {
        ConsistentHashRing ring = new ConsistentHashRing(100);

        ring.addNode("node-A");
        ring.addNode("node-B");

        assertEquals(2, ring.getNodeCount(), "Node count");
        assertEquals(200, ring.getVirtualNodeCount(),
                "Virtual node count");

        System.out.println("PASS: Node addition");
    }

    private static void testKeyRouting() {
        ConsistentHashRing ring = new ConsistentHashRing(100);

        ring.addNode("node-A");
        ring.addNode("node-B");
        ring.addNode("node-C");

        for (int i = 0; i < 1_000; i++) {
            String node = ring.getNode("key:" + i);

            if (!ring.containsNode(node)) {
                throw new AssertionError(
                        "Key routed to an unknown node: " + node);
            }
        }

        System.out.println("PASS: Key routing");
    }

    private static void testDuplicateNode() {
        ConsistentHashRing ring = new ConsistentHashRing(50);

        ring.addNode("node-A");
        ring.addNode("node-A");

        assertEquals(1, ring.getNodeCount(), "Duplicate node count");
        assertEquals(50, ring.getVirtualNodeCount(),
                "Duplicate virtual nodes");

        System.out.println("PASS: Duplicate node handling");
    }

    private static void testNodeRemoval() {
        ConsistentHashRing ring = new ConsistentHashRing(100);

        ring.addNode("node-A");
        ring.addNode("node-B");
        ring.removeNode("node-A");

        assertEquals(1, ring.getNodeCount(), "Remaining nodes");

        if (!ring.containsNode("node-B")) {
            throw new AssertionError("node-B should remain");
        }

        for (int i = 0; i < 1_000; i++) {
            assertEquals(
                    "node-B",
                    ring.getNode("key:" + i),
                    "Routing after removal");
        }

        System.out.println("PASS: Node removal");
    }

    private static void testVirtualNodes() {
        ConsistentHashRing ring = new ConsistentHashRing(128);

        ring.addNode("node-A");
        ring.addNode("node-B");

        assertEquals(256, ring.getVirtualNodeCount(),
                "Virtual node total");

        System.out.println("PASS: Virtual nodes");
    }

    private static void testDistribution() {
        ConsistentHashRing ring = new ConsistentHashRing(128);

        ring.addNode("node-A");
        ring.addNode("node-B");
        ring.addNode("node-C");

        Map<String, Integer> counts = new HashMap<>();

        for (int i = 0; i < 30_000; i++) {
            String node = ring.getNode("distribution-key:" + i);
            counts.merge(node, 1, Integer::sum);
        }

        System.out.println("Distribution across 30,000 keys:");

        for (String node : ring.getNodes()) {
            int count = counts.getOrDefault(node, 0);
            double percentage = count * 100.0 / 30_000;

            System.out.printf(
                    "  %s: %d keys (%.2f%%)%n",
                    node, count, percentage);
        }

        if (counts.size() != 3) {
            throw new AssertionError(
                    "Expected all three nodes to receive keys");
        }

        System.out.println("PASS: Key distribution");
    }

    private static void assertEquals(
            Object expected,
            Object actual,
            String description) {

        if (!expected.equals(actual)) {
            throw new AssertionError(
                    description + ": expected " + expected
                            + ", got " + actual);
        }
    }

    private static void expectThrows(
            Class<? extends Throwable> expected,
            Runnable action) {

        try {
            action.run();
        } catch (Throwable actual) {
            if (expected.isInstance(actual)) {
                return;
            }

            throw new AssertionError(
                    "Expected " + expected.getSimpleName()
                            + ", got " + actual, actual);
        }

        throw new AssertionError(
                "Expected " + expected.getSimpleName());
    }
}
