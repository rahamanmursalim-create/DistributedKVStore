package com.codecrafters.redis.distributed;

import com.codecrafters.redis.storage.StorageEngine;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;

/**
 * Multi-node integration tests verifying storage, replication,
 * routing, failure handling, and recovery working together.
 */
public final class MultiNodeIntegrationTest {

    private MultiNodeIntegrationTest() {
    }

    public static void main(String[] args) throws Exception {
        Path root = Files.createTempDirectory("multi-node-");
        try {
            testWriteReadCycle(root);
            testRepeatedUpdates(root);
            testDeleteKey(root);
            testRouterWithMultipleKeys(root);
            testReplicationManagerWriteAndRead(root);
            testReplicaRestart(root);
            testNodeFailureHandling(root);
            testNodeAdditionRemovalRouting(root);
            testCompactionFollowedByReads(root);
            testWalRecoveryAfterRestart(root);

            System.out.println(
                    "\nAll multi-node integration tests passed.");
        } finally {
            deleteDirectory(root);
        }
    }

    private static void testWriteReadCycle(Path root) throws Exception {
        Path dir = root.resolve("write-read");
        try (StorageEngine engine = new StorageEngine(dir)) {
            engine.put("key", "value");
            check("value".equals(engine.get("key")),
                    "Write-read cycle failed");
        }
        System.out.println("PASS: Write followed by read");
    }

    private static void testRepeatedUpdates(Path root) throws Exception {
        Path dir = root.resolve("repeated-updates");
        try (StorageEngine engine = new StorageEngine(dir)) {
            for (int i = 0; i < 10; i++) {
                engine.put("key", "v" + i);
            }
            check("v9".equals(engine.get("key")),
                    "Should see last update");
            engine.flush();
            check("v9".equals(engine.get("key")),
                    "Last update should survive flush");
        }
        System.out.println("PASS: Repeated updates to same key");
    }

    private static void testDeleteKey(Path root) throws Exception {
        Path dir = root.resolve("delete-key");
        try (StorageEngine engine = new StorageEngine(dir)) {
            engine.put("key", "value");
            engine.flush();
            check(engine.delete("key"), "Delete should return true");
            check(engine.get("key") == null,
                    "Key should be null after delete");
            engine.flush();

            // Restart and verify.
            engine.close();
            try (StorageEngine reopened = new StorageEngine(dir)) {
                check(reopened.get("key") == null,
                        "Deleted key should stay null after restart");
            }
        }
        System.out.println("PASS: Delete key and persistence");
    }

    private static void testRouterWithMultipleKeys(Path root)
            throws Exception {

        ConsistentHashRing ring = new ConsistentHashRing(100);
        DistributedNodeRegistry registry = new DistributedNodeRegistry();
        DistributedKeyRouter router =
                new DistributedKeyRouter(ring, registry);

        StorageNode nodeA =
                new StorageNode("node-A", "127.0.0.1", 7001);
        StorageNode nodeB =
                new StorageNode("node-B", "127.0.0.1", 7002);
        StorageNode nodeC =
                new StorageNode("node-C", "127.0.0.1", 7003);

        router.addNode(nodeA);
        router.addNode(nodeB);
        router.addNode(nodeC);

        int routedToA = 0, routedToB = 0, routedToC = 0;

        for (int i = 0; i < 300; i++) {
            StorageNode node = router.routeKey("key-" + i);
            if ("node-A".equals(node.getNodeId())) routedToA++;
            else if ("node-B".equals(node.getNodeId())) routedToB++;
            else if ("node-C".equals(node.getNodeId())) routedToC++;
        }

        check(routedToA > 0 && routedToB > 0 && routedToC > 0,
                "All nodes should receive some keys");
        check(routedToA + routedToB + routedToC == 300,
                "Total routed keys should be 300");

        System.out.println("PASS: Router distributes keys to multiple nodes");
    }

    private static void testReplicationManagerWriteAndRead(Path root)
            throws Exception {

        Path dirA = root.resolve("repl-mgr-A");
        Path dirB = root.resolve("repl-mgr-B");

        try (StorageEngine engineA = new StorageEngine(dirA);
             StorageEngine engineB = new StorageEngine(dirB)) {

            ReplicationManager mgr = new ReplicationManager(2);
            mgr.registerEngine("A", engineA);
            mgr.registerEngine("B", engineB);

            mgr.replicatePut(List.of("A", "B"), "shared", "value", -1L);

            check("value".equals(mgr.readFromNode("A", "shared")),
                    "Node A should have replicated value");
            check("value".equals(mgr.readFromNode("B", "shared")),
                    "Node B should have replicated value");
        }
        System.out.println("PASS: ReplicationManager write and read");
    }

    private static void testReplicaRestart(Path root) throws Exception {
        Path targetDir = root.resolve("replica-restart");
        int port = findFreePort();

        // Phase 1: replicate data and flush.
        {
            StorageEngine target = new StorageEngine(targetDir);
            ReplicationServer server =
                    new ReplicationServer(port, target);
            Thread serverThread = startServer(server);

            try {
                Thread.sleep(100);
                ReplicationClient client =
                        new ReplicationClient("127.0.0.1", port);

                client.replicatePut("restart-key", "restart-val", -1L);
                target.flush();
            } finally {
                server.close();
                target.close();
                serverThread.join(2000);
            }
        }

        // Phase 2: reopen and verify.
        try (StorageEngine reopened = new StorageEngine(targetDir)) {
            check("restart-val".equals(reopened.get("restart-key")),
                    "Replicated key should survive replica restart");
        }
        System.out.println("PASS: Replica restart and recovery");
    }

    private static void testNodeFailureHandling(Path root) throws Exception {
        ConsistentHashRing ring = new ConsistentHashRing(50);
        DistributedNodeRegistry registry = new DistributedNodeRegistry();
        DistributedKeyRouter router =
                new DistributedKeyRouter(ring, registry);

        StorageNode nodeA =
                new StorageNode("node-A", "127.0.0.1", 8001);
        StorageNode nodeB =
                new StorageNode("node-B", "127.0.0.1", 8002);

        router.addNode(nodeA);
        router.addNode(nodeB);

        // Deactivate node-A.
        nodeA.deactivate();

        // All routes should go to node-B now.
        for (int i = 0; i < 50; i++) {
            StorageNode routed = router.routeKey("fail-key-" + i);
            check(routed.isActive(),
                    "Routed node should be active");
        }

        // Deactivate all nodes.
        nodeB.deactivate();
        boolean threw = false;
        try {
            router.routeKey("any-key");
        } catch (IllegalStateException e) {
            threw = true;
        }
        check(threw,
                "No active nodes should throw IllegalStateException");

        // Reactivate node-A.
        nodeA.activate();
        StorageNode reactivated = router.routeKey("any-key");
        check("node-A".equals(reactivated.getNodeId()),
                "Reactivated node should be routable");

        System.out.println(
                "PASS: Node failure and reactivation handling");
    }

    private static void testNodeAdditionRemovalRouting(Path root)
            throws Exception {

        ConsistentHashRing ring = new ConsistentHashRing(100);
        DistributedNodeRegistry registry = new DistributedNodeRegistry();
        DistributedKeyRouter router =
                new DistributedKeyRouter(ring, registry);

        StorageNode nodeA =
                new StorageNode("node-A", "127.0.0.1", 9001);
        router.addNode(nodeA);

        // All keys go to node-A.
        String beforeRoute = router.routeKey("test-key").getNodeId();
        check("node-A".equals(beforeRoute),
                "Only node should handle the key");

        // Add node-B.
        StorageNode nodeB =
                new StorageNode("node-B", "127.0.0.1", 9002);
        router.addNode(nodeB);

        // Remove node-A.
        router.removeNode("node-A");
        String afterRoute = router.routeKey("test-key").getNodeId();
        check("node-B".equals(afterRoute),
                "After removing A, B should handle all keys");

        System.out.println(
                "PASS: Node addition and removal routing");
    }

    private static void testCompactionFollowedByReads(Path root)
            throws Exception {

        Path dir = root.resolve("compaction-reads");
        try (StorageEngine engine = new StorageEngine(dir)) {
            engine.put("a", "1");
            engine.flush();
            engine.put("b", "2");
            engine.flush();
            engine.put("a", "updated");
            engine.flush();

            check(engine.sstableCount() >= 2,
                    "Should have multiple SSTables");
            engine.compact();
            check(engine.sstableCount() == 1,
                    "Should have 1 SSTable after compaction");

            check("updated".equals(engine.get("a")),
                    "Latest value should survive compaction");
            check("2".equals(engine.get("b")),
                    "Other key should survive compaction");
        }
        System.out.println("PASS: Compaction followed by reads");
    }

    private static void testWalRecoveryAfterRestart(Path root)
            throws Exception {

        Path dir = root.resolve("wal-recovery");

        // Write without flushing.
        {
            StorageEngine engine = new StorageEngine(dir);
            engine.put("unflushed-key", "unflushed-value");
            engine.close();
        }

        // Reopen and verify WAL recovery.
        try (StorageEngine engine = new StorageEngine(dir)) {
            check("unflushed-value".equals(engine.get("unflushed-key")),
                    "Unflushed key should be recovered from WAL");
        }
        System.out.println("PASS: WAL recovery after restart");
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static Thread startServer(ReplicationServer server) {
        Thread t = new Thread(() -> {
            try {
                server.start();
            } catch (IOException e) {
                // Server stopped.
            }
        });
        t.setDaemon(true);
        t.start();
        return t;
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static void deleteDirectory(Path dir) throws Exception {
        if (Files.exists(dir)) {
            try (var paths = Files.walk(dir)) {
                paths.sorted(Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            } catch (Exception e) {
                                // Best effort.
                            }
                        });
            }
        }
    }
}
