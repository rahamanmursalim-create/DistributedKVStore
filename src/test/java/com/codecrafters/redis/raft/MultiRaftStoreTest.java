package com.codecrafters.redis.raft;

import com.codecrafters.redis.storage.RedisStore;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public final class MultiRaftStoreTest {
    public static void main(String[] args) throws Exception {
        Path root = Files.createTempDirectory("multi-raft-store-test");
        String[] nodeIds = {"n1", "n2", "n3"};
        String[] groupIds = {"shard-a", "shard-b"};
        int[][] ports = new int[groupIds.length][nodeIds.length];
        Set<Integer> reserved = new HashSet<>();
        RaftGroup[][] groups = new RaftGroup[groupIds.length][nodeIds.length];
        RedisStore[][] stores = new RedisStore[groupIds.length][nodeIds.length];
        MultiRaftStore routed = null;
        try {
            for (int g = 0; g < groupIds.length; g++)
                for (int n = 0; n < nodeIds.length; n++) ports[g][n] = availablePort(reserved);
            for (int g = 0; g < groupIds.length; g++) {
                for (int n = 0; n < nodeIds.length; n++) {
                    Map<String, InetSocketAddress> peers = new LinkedHashMap<>();
                    for (int peer = 0; peer < nodeIds.length; peer++) if (peer != n)
                        peers.put(nodeIds[peer], new InetSocketAddress("127.0.0.1", ports[g][peer]));
                    Path dir = root.resolve(groupIds[g] + "-" + nodeIds[n]);
                    stores[g][n] = new RedisStore(dir.resolve("state.wal"), false);
                    groups[g][n] = new RaftGroup(groupIds[g], nodeIds[n],
                            new InetSocketAddress("127.0.0.1", ports[g][n]), peers, dir, stores[g][n]);
                }
            }
            for (RaftGroup[] group : groups) for (RaftGroup node : group) node.start();
            int[] leaderIndexes = {awaitLeader(groups[0]), awaitLeader(groups[1])};
            assert leaderIndexes[0] >= 0 && leaderIndexes[1] >= 0;
            Map<String, RaftGroup> leaders = Map.of(
                    groupIds[0], groups[0][leaderIndexes[0]],
                    groupIds[1], groups[1][leaderIndexes[1]]);
            routed = new MultiRaftStore(leaders, 64, false);

            Map<String, String> batch = new LinkedHashMap<>();
            for (int i = 0; i < 100; i++) batch.put("multi-key-" + i, "value-" + i);
            routed.setBatch(batch);
            assert routed.keys("multi-key-*").size() == 100;
            for (int i = 0; i < 100; i++) {
                String key = "multi-key-" + i;
                String value = "value-" + i;
                assert value.equals(routed.get(key));
                int ownerGroup = groupIds[0].equals(routed.groupFor(key)) ? 0 : 1;
                for (int node = 0; node < nodeIds.length; node++) {
                    assert value.equals(stores[ownerGroup][node].get(key));
                    assert stores[1 - ownerGroup][node].get(key) == null;
                }
            }
            assert routed.delete("multi-key-1", "multi-key-2") == 2;
            assert routed.get("multi-key-1") == null;
            System.out.println("MultiRaftStoreTest passed");
        } finally {
            for (RaftGroup[] group : groups) for (RaftGroup node : group)
                if (node != null) try { node.close(); } catch (Exception ignored) { }
            for (RedisStore[] group : stores) for (RedisStore store : group)
                if (store != null) store.close();
        }
    }

    private static int availablePort(Set<Integer> reserved) throws Exception {
        while (true) {
            try (java.net.ServerSocket socket = new java.net.ServerSocket(0)) {
                int port = socket.getLocalPort();
                if (reserved.add(port)) return port;
            }
        }
    }

    private static int awaitLeader(RaftGroup[] nodes) throws InterruptedException {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(8);
        while (System.nanoTime() < deadline) {
            for (int i = 0; i < nodes.length; i++) if (nodes[i].isLeader()) return i;
            Thread.sleep(25);
        }
        return -1;
    }
}
