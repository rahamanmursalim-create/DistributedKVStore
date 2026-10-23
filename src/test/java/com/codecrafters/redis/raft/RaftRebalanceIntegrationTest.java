package com.codecrafters.redis.raft;

import com.codecrafters.redis.storage.RedisStore;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** Exercises quorum-fenced data movement between real three-node Raft groups. */
public final class RaftRebalanceIntegrationTest {
    public static void main(String[] args) throws Exception {
        Path root = Files.createTempDirectory("raft-rebalance-integration");
        String[] nodeIds = {"n1", "n2", "n3"};
        String[] groupIds = {"a", "b"};
        int[][] ports = new int[groupIds.length][nodeIds.length];
        Set<Integer> reservedPorts = new HashSet<>();
        RaftGroup[][] groups = new RaftGroup[groupIds.length][nodeIds.length];
        RedisStore[][] stores = new RedisStore[groupIds.length][nodeIds.length];
        try {
            for (int group = 0; group < groupIds.length; group++) {
                for (int node = 0; node < nodeIds.length; node++)
                    ports[group][node] = availablePort(reservedPorts);
            }
            for (int group = 0; group < groupIds.length; group++) {
                for (int node = 0; node < nodeIds.length; node++) {
                    Map<String, InetSocketAddress> peers = new LinkedHashMap<>();
                    for (int peer = 0; peer < nodeIds.length; peer++) {
                        if (peer != node)
                            peers.put(nodeIds[peer], new InetSocketAddress("127.0.0.1", ports[group][peer]));
                    }
                    Path directory = root.resolve(groupIds[group] + "-" + nodeIds[node]);
                    stores[group][node] = new RedisStore(directory.resolve("state.wal"), false);
                    groups[group][node] = new RaftGroup(groupIds[group], nodeIds[node],
                            new InetSocketAddress("127.0.0.1", ports[group][node]), peers,
                            directory, stores[group][node]);
                }
            }
            for (RaftGroup[] group : groups) for (RaftGroup node : group) node.start();

            Map<String, RaftGroup> leaders = awaitLeaders(groups, groupIds);
            // Give the routers non-leader handles to exercise admin/read forwarding.
            Map<String, RaftGroup> followers = new LinkedHashMap<>();
            for (int group = 0; group < groupIds.length; group++) {
                RaftGroup leader = leaders.get(groupIds[group]);
                for (RaftGroup candidate : groups[group]) if (candidate != leader) {
                    followers.put(groupIds[group], candidate); break;
                }
            }
            MultiRaftStore routed = new MultiRaftStore(followers, 64, false);
            MultiRaftStore staleRouter = new MultiRaftStore(followers, 64, false);
            Map<String, String> values = new LinkedHashMap<>();
            for (int candidate = 0; values.size() < 80; candidate++) {
                String key = "rebalance-" + candidate;
                if ("b".equals(routed.groupFor(key))) values.put(key, "value-" + candidate);
            }
            routed.setBatch(values);
            String expiringKey = values.keySet().iterator().next();
            routed.set(expiringKey, "ttl-value", 120_000);

            routed.rebalanceGroups(java.util.List.of("a"));

            boolean staleWriteRejected = false;
            try { staleRouter.set(expiringKey, "stale-owner-write"); }
            catch (java.io.UncheckedIOException expected) {
                staleWriteRejected = expected.getCause().getMessage().contains("Stale shard owner");
            }
            if (!staleWriteRejected) throw new AssertionError("Old routing epoch was allowed to write after migration");
            boolean staleReadRejected = false;
            try { staleRouter.get(expiringKey); }
            catch (java.io.UncheckedIOException expected) {
                staleReadRejected = expected.getCause().getMessage().contains("Stale shard owner");
            }
            if (!staleReadRejected) throw new AssertionError("Old routing epoch was allowed to read after migration");
            MultiRaftStore restoredRouter = new MultiRaftStore(followers, 64, false);
            if (!"a".equals(restoredRouter.groupFor(expiringKey)))
                throw new AssertionError("A newly created router ignored the committed ownership manifest");

            if (!"a".equals(routed.groupFor(expiringKey)))
                throw new AssertionError("Key routing did not switch to the new group set");
            for (Map.Entry<String, String> entry : values.entrySet()) {
                String expected = entry.getKey().equals(expiringKey) ? "ttl-value" : entry.getValue();
                if (!expected.equals(routed.get(entry.getKey())))
                    throw new AssertionError("Rebalanced read returned the wrong value for " + entry.getKey());
            }
            for (int node = 0; node < nodeIds.length; node++) {
                if (stores[1][node].size() != 0)
                    throw new AssertionError("Source replica retained keys on " + nodeIds[node]);
                for (Map.Entry<String, String> entry : values.entrySet()) {
                    String expected = entry.getKey().equals(expiringKey) ? "ttl-value" : entry.getValue();
                    if (!expected.equals(stores[0][node].get(entry.getKey())))
                        throw new AssertionError("Destination replica missed " + entry.getKey()
                                + " on " + nodeIds[node]);
                }
            }
            routed.set("post-rebalance", "writes-resumed");
            if (!"writes-resumed".equals(routed.get("post-rebalance")))
                throw new AssertionError("Writes did not resume after rebalance");

            for (RaftGroup[] group : groups) for (int node = 0; node < group.length; node++) {
                if (group[node] != null) { group[node].close(); group[node] = null; }
                if (stores[0][node] != null && node == 0) { stores[0][node].close(); stores[0][node] = null; }
                if (stores[1][node] != null && node == 0) { stores[1][node].close(); stores[1][node] = null; }
            }
            Path recoveredDirectory = root.resolve("a-n1");
            RedisStore recoveredStore = new RedisStore(recoveredDirectory.resolve("state.wal"), false);
            try (RaftGroup recovered = new RaftGroup("a", "n1", new InetSocketAddress("127.0.0.1", ports[0][0]),
                    Map.of("n2", new InetSocketAddress("127.0.0.1", ports[0][1]),
                            "n3", new InetSocketAddress("127.0.0.1", ports[0][2])), recoveredDirectory, recoveredStore)) {
                if (recovered.getOwnershipEpoch() != 1 || !recovered.getOwnershipGroups().equals(java.util.List.of("a")))
                    throw new AssertionError("Ownership epoch/manifest did not survive Raft restart");
            } finally { recoveredStore.close(); }
            System.out.println("RaftRebalanceIntegrationTest passed: moved=80, replicas=3, ttl=preserved");
        } finally {
            for (RaftGroup[] group : groups) for (RaftGroup node : group)
                if (node != null) try { node.close(); } catch (Exception ignored) { }
            for (RedisStore[] group : stores) for (RedisStore store : group)
                if (store != null) store.close();
            deleteTree(root);
        }
    }

    private static Map<String, RaftGroup> awaitLeaders(RaftGroup[][] groups, String[] groupIds)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        Map<String, RaftGroup> leaders = new LinkedHashMap<>();
        while (System.nanoTime() < deadline && leaders.size() < groups.length) {
            for (int group = 0; group < groups.length; group++) {
                for (RaftGroup node : groups[group]) {
                    if (node.isLeader()) leaders.put(groupIds[group], node);
                }
            }
            Thread.sleep(20);
        }
        if (leaders.size() != groups.length) throw new AssertionError("Raft groups did not elect leaders");
        return leaders;
    }

    private static int availablePort(Set<Integer> reserved) throws Exception {
        while (true) {
            try (ServerSocket socket = new ServerSocket(0)) {
                int port = socket.getLocalPort();
                if (reserved.add(port)) return port;
            }
        }
    }

    private static void deleteTree(Path root) throws Exception {
        if (!Files.exists(root)) return;
        try (var paths = Files.walk(root)) {
            for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) Files.deleteIfExists(path);
        }
    }
}
