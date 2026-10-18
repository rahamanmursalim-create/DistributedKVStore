package com.codecrafters.redis.distributed;

public final class DistributedKeyRouterTest {

    public static void main(String[] args) {
        testAddAndRoute();
        testRemoveNode();
        testDuplicateNode();
        testNoNodes();
        testInactiveNode();
        testActiveNodeList();
        testFallbackToActiveNode();

        System.out.println("All DistributedKeyRouter tests passed.");
    }

    private static void testAddAndRoute() {
        ConsistentHashRing ring = new ConsistentHashRing(100);
        DistributedNodeRegistry registry = new DistributedNodeRegistry();
        DistributedKeyRouter router =
                new DistributedKeyRouter(ring, registry);

        router.addNode(new StorageNode("node-A", "127.0.0.1", 7001));
        router.addNode(new StorageNode("node-B", "127.0.0.1", 7002));

        StorageNode node = router.routeKey("customer:101");

        check(registry.containsNode(node.getNodeId()),
                "Routed node must be registered");

        System.out.println("PASS: Add and route");
    }

    private static void testRemoveNode() {
        ConsistentHashRing ring = new ConsistentHashRing(100);
        DistributedNodeRegistry registry = new DistributedNodeRegistry();
        DistributedKeyRouter router =
                new DistributedKeyRouter(ring, registry);

        router.addNode(new StorageNode("node-A", "127.0.0.1", 7001));

        check(router.removeNode("node-A"),
                "Existing node should be removed");
        check(!router.removeNode("node-A"),
                "Removed node should not exist");

        expectThrows(IllegalStateException.class,
                () -> router.routeKey("customer:101"));

        System.out.println("PASS: Remove node");
    }

    private static void testDuplicateNode() {
        ConsistentHashRing ring = new ConsistentHashRing(100);
        DistributedNodeRegistry registry = new DistributedNodeRegistry();
        DistributedKeyRouter router =
                new DistributedKeyRouter(ring, registry);

        router.addNode(new StorageNode("node-A", "127.0.0.1", 7001));

        expectThrows(IllegalArgumentException.class,
                () -> router.addNode(
                        new StorageNode("node-A", "127.0.0.1", 7002)));

        check(registry.getNodeCount() == 1,
                "Duplicate node must not be added");

        System.out.println("PASS: Duplicate node");
    }

    private static void testNoNodes() {
        DistributedKeyRouter router = new DistributedKeyRouter(
                new ConsistentHashRing(100),
                new DistributedNodeRegistry());

        expectThrows(IllegalStateException.class,
                () -> router.routeKey("customer:101"));

        System.out.println("PASS: No nodes");
    }

    private static void testInactiveNode() {
        ConsistentHashRing ring = new ConsistentHashRing(100);
        DistributedNodeRegistry registry = new DistributedNodeRegistry();
        DistributedKeyRouter router =
                new DistributedKeyRouter(ring, registry);

        StorageNode nodeA =
                new StorageNode("node-A", "127.0.0.1", 7001);
        StorageNode nodeB =
                new StorageNode("node-B", "127.0.0.1", 7002);

        router.addNode(nodeA);
        router.addNode(nodeB);

        nodeA.deactivate();
        nodeB.deactivate();

        expectThrows(IllegalStateException.class,
                () -> router.routeKey("customer:101"));

        nodeA.activate();

        StorageNode routed = router.routeKey("customer:101");
        check(routed.isActive(), "Routed node must be active");

        System.out.println("PASS: Inactive-node handling");
    }

    private static void testActiveNodeList() {
        ConsistentHashRing ring = new ConsistentHashRing(100);
        DistributedNodeRegistry registry = new DistributedNodeRegistry();
        DistributedKeyRouter router =
                new DistributedKeyRouter(ring, registry);

        StorageNode nodeA =
                new StorageNode("node-A", "127.0.0.1", 7001);
        StorageNode nodeB =
                new StorageNode("node-B", "127.0.0.1", 7002);

        router.addNode(nodeA);
        router.addNode(nodeB);
        nodeA.deactivate();

        check(router.getActiveNodes().size() == 1,
                "Only one node should be active");

        System.out.println("PASS: Active-node list");
    }

    private static void testFallbackToActiveNode() {
        ConsistentHashRing ring = new ConsistentHashRing(100);
        DistributedNodeRegistry registry = new DistributedNodeRegistry();
        DistributedKeyRouter router =
                new DistributedKeyRouter(ring, registry);

        StorageNode nodeA =
                new StorageNode("node-A", "127.0.0.1", 7001);
        StorageNode nodeB =
                new StorageNode("node-B", "127.0.0.1", 7002);

        router.addNode(nodeA);
        router.addNode(nodeB);

        // Find a key originally assigned to node-A.
        String key = null;

        for (int i = 0; i < 100_000; i++) {
            String candidate = "failover-key-" + i;

            if (ring.getNode(candidate).equals("node-A")) {
                key = candidate;
                break;
            }
        }

        check(key != null,
                "Must find a key assigned to node-A");

        // Simulate node-A becoming inactive.
        nodeA.deactivate();

        StorageNode routed = router.routeKey(key);

        check(routed.isActive(),
                "Router must return an active node");

        check(routed.getNodeId().equals("node-B"),
                "Router should fall back to node-B");

        System.out.println("PASS: Fallback to active node");
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
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
                            + " but got " + actual, actual);
        }

        throw new AssertionError(
                "Expected exception: " + expected.getSimpleName());
    }
}