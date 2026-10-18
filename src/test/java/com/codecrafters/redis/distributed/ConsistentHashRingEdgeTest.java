package com.codecrafters.redis.distributed;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Edge-case tests for ConsistentHashRing.
 */
public final class ConsistentHashRingEdgeTest {

    private ConsistentHashRingEdgeTest() {
    }

    public static void main(String[] args) {
        testSingleNode();
        testRemoveAllNodes();
        testAddRemoveAddBack();
        testLargeNodeCount();
        testInvalidConstructorArgs();
        testInvalidNodeId();
        testNodeCountAccuracy();
        testContainsNode();
        testGetNodesSorted();

        System.out.println(
                "\nAll ConsistentHashRing edge tests passed.");
    }

    private static void testSingleNode() {
        ConsistentHashRing ring = new ConsistentHashRing(100);
        ring.addNode("only-node");

        for (int i = 0; i < 100; i++) {
            String node = ring.getNode("key-" + i);
            check("only-node".equals(node),
                    "All keys should route to the only node");
        }
        System.out.println(
                "PASS: Single node handles all keys");
    }

    private static void testRemoveAllNodes() {
        ConsistentHashRing ring = new ConsistentHashRing(50);
        ring.addNode("node-A");
        ring.addNode("node-B");
        ring.removeNode("node-A");
        ring.removeNode("node-B");

        boolean threw = false;
        try {
            ring.getNode("key");
        } catch (IllegalStateException e) {
            threw = true;
        }
        check(threw,
                "getNode on empty ring should throw IllegalStateException");
        System.out.println(
                "PASS: Remove all nodes, getNode throws");
    }

    private static void testAddRemoveAddBack() {
        ConsistentHashRing ring = new ConsistentHashRing(50);
        ring.addNode("node-A");

        String before = ring.getNode("test-key");
        ring.removeNode("node-A");
        ring.addNode("node-A");

        String after = ring.getNode("test-key");
        check("node-A".equals(before) && "node-A".equals(after),
                "Re-added node should route the same way");
        System.out.println(
                "PASS: Add, remove, add back works");
    }

    private static void testLargeNodeCount() {
        ConsistentHashRing ring = new ConsistentHashRing(100);
        for (int i = 0; i < 10; i++) {
            ring.addNode("node-" + i);
        }

        Map<String, Integer> distribution = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            String node = ring.getNode("key-" + i);
            distribution.merge(node, 1, Integer::sum);
        }

        check(distribution.size() == 10,
                "All 10 nodes should receive traffic");

        for (Map.Entry<String, Integer> entry : distribution.entrySet()) {
            check(entry.getValue() > 0,
                    "Node " + entry.getKey() + " should have keys");
        }
        System.out.println(
                "PASS: 10 nodes with reasonable distribution");
    }

    private static void testInvalidConstructorArgs() {
        expectThrows(() -> new ConsistentHashRing(0),
                IllegalArgumentException.class,
                "Zero virtual nodes");
        expectThrows(() -> new ConsistentHashRing(-1),
                IllegalArgumentException.class,
                "Negative virtual nodes");
        System.out.println(
                "PASS: Invalid constructor args rejected");
    }

    private static void testInvalidNodeId() {
        ConsistentHashRing ring = new ConsistentHashRing(10);

        expectThrows(() -> ring.addNode(null),
                RuntimeException.class,
                "Null nodeId");
        expectThrows(() -> ring.addNode(""),
                RuntimeException.class,
                "Empty nodeId");
        expectThrows(() -> ring.addNode("   "),
                RuntimeException.class,
                "Blank nodeId");
        System.out.println(
                "PASS: Invalid node IDs rejected");
    }

    private static void testNodeCountAccuracy() {
        ConsistentHashRing ring = new ConsistentHashRing(50);
        check(ring.getNodeCount() == 0,
                "Initial node count should be 0");
        check(ring.getVirtualNodeCount() == 0,
                "Initial virtual count should be 0");

        ring.addNode("A");
        check(ring.getNodeCount() == 1, "Count after add A");
        check(ring.getVirtualNodeCount() == 50,
                "Virtual count after add A");

        ring.addNode("B");
        check(ring.getNodeCount() == 2, "Count after add B");
        check(ring.getVirtualNodeCount() == 100,
                "Virtual count after add B");

        ring.removeNode("A");
        check(ring.getNodeCount() == 1, "Count after remove A");
        check(ring.getVirtualNodeCount() == 50,
                "Virtual count after remove A");

        System.out.println(
                "PASS: Node counts accurate after adds and removes");
    }

    private static void testContainsNode() {
        ConsistentHashRing ring = new ConsistentHashRing(10);
        check(!ring.containsNode("A"),
                "Should not contain non-added node");
        ring.addNode("A");
        check(ring.containsNode("A"),
                "Should contain added node");
        ring.removeNode("A");
        check(!ring.containsNode("A"),
                "Should not contain removed node");
        System.out.println(
                "PASS: containsNode correct results");
    }

    private static void testGetNodesSorted() {
        ConsistentHashRing ring = new ConsistentHashRing(10);
        ring.addNode("C");
        ring.addNode("A");
        ring.addNode("B");

        Collection<String> nodes = ring.getNodes();
        String[] sorted = nodes.toArray(new String[0]);
        check("A".equals(sorted[0]) && "B".equals(sorted[1])
                        && "C".equals(sorted[2]),
                "getNodes should return sorted collection");

        // Verify unmodifiable.
        boolean unmodifiable = false;
        try {
            nodes.add("D");
        } catch (UnsupportedOperationException e) {
            unmodifiable = true;
        }
        check(unmodifiable,
                "getNodes should return unmodifiable collection");
        System.out.println(
                "PASS: getNodes sorted and unmodifiable");
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static void expectThrows(
            Runnable action,
            Class<? extends Throwable> expected,
            String description) {

        boolean threw = false;
        try {
            action.run();
        } catch (Throwable t) {
            if (expected.isInstance(t)) {
                threw = true;
            } else {
                throw new AssertionError(
                        description + ": expected " + expected.getSimpleName()
                                + " but got " + t.getClass().getSimpleName(),
                        t);
            }
        }
        if (!threw) {
            throw new AssertionError(
                    description + ": expected " + expected.getSimpleName()
                            + " but no exception was thrown");
        }
    }
}
