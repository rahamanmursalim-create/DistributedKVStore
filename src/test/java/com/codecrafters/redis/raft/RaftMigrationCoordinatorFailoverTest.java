package com.codecrafters.redis.raft;

import org.junit.jupiter.api.Test;

import com.codecrafters.redis.storage.RedisStore;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** Verifies a newly elected first-group leader resumes a committed migration intent. */
public final class RaftMigrationCoordinatorFailoverTest {
    public static void main(String[] args) throws Exception {
        Path root = Files.createTempDirectory("raft-migration-coordinator-failover");
        String[] nodes = {"n1", "n2", "n3"};
        String[] groupIds = {"a", "b"};
        int[][] ports = new int[groupIds.length][nodes.length];
        RaftGroup[][] groups = new RaftGroup[groupIds.length][nodes.length];
        RedisStore[][] stores = new RedisStore[groupIds.length][nodes.length];
        MultiRaftStore original = null;
        MultiRaftStore takeover = null;
        try {
            Set<Integer> reserved = new java.util.HashSet<>();
            for (int group = 0; group < groupIds.length; group++) for (int node = 0; node < nodes.length; node++) {
                try (ServerSocket socket = new ServerSocket(0)) {
                    int port = socket.getLocalPort();
                    while (!reserved.add(port)) port++;
                    ports[group][node] = port;
                }
            }
            for (int group = 0; group < groupIds.length; group++) for (int node = 0; node < nodes.length; node++) {
                Map<String, InetSocketAddress> peers = new LinkedHashMap<>();
                for (int peer = 0; peer < nodes.length; peer++) if (peer != node)
                    peers.put(nodes[peer], new InetSocketAddress("127.0.0.1", ports[group][peer]));
                Path directory = root.resolve(groupIds[group] + "-" + nodes[node]);
                stores[group][node] = new RedisStore(directory.resolve("state.wal"), false);
                groups[group][node] = new RaftGroup(groupIds[group], nodes[node],
                        new InetSocketAddress("127.0.0.1", ports[group][node]), peers, directory, stores[group][node]);
            }
            for (RaftGroup[] group : groups) for (RaftGroup node : group) node.start();
            int leaderNode = awaitLeader(groups[0]);
            awaitLeader(groups[1]);
            Map<String, RaftGroup> localGroups = new LinkedHashMap<>();
            localGroups.put("a", groups[0][leaderNode]); localGroups.put("b", groups[1][leaderNode]);
            original = new MultiRaftStore(localGroups, 64, false);
            String movedKey = null;
            for (int i = 0; movedKey == null; i++) {
                String key = "failover-key-" + i;
                if ("b".equals(original.groupFor(key))) movedKey = key;
            }
            original.set(movedKey, "durable-value");

            RaftGroup failedCoordinator = groups[0][leaderNode];
            failedCoordinator.setMigrationIntent(java.util.List.of("a"));
            failedCoordinator.setMaintenanceFrozen(true);
            failedCoordinator.close();
            groups[0][leaderNode] = null;

            long electionDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(12);
            int newLeader = -1;
            while (System.nanoTime() < electionDeadline) {
                for (int node = 0; node < nodes.length; node++)
                    if (groups[0][node] != null && groups[0][node].isLeader()) newLeader = node;
                if (newLeader >= 0) break;
                Thread.sleep(20);
            }
            if (newLeader < 0) throw new AssertionError("First Raft group did not elect a replacement coordinator");
            Map<String, RaftGroup> replacementGroups = new LinkedHashMap<>();
            replacementGroups.put("a", groups[0][newLeader]); replacementGroups.put("b", groups[1][newLeader]);
            takeover = new MultiRaftStore(replacementGroups, 64, false);

            long recoveryDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (System.nanoTime() < recoveryDeadline) {
                boolean complete = true;
                for (RaftGroup[] group : groups) for (RaftGroup node : group)
                    if (node != null && (node.isMaintenanceFrozen()
                            || !node.getOwnershipGroups().equals(java.util.List.of("a")))) complete = false;
                boolean replicatedValue = true;
                for (int node = 0; node < nodes.length; node++)
                    if (groups[0][node] != null && !"durable-value".equals(stores[0][node].get(movedKey)))
                        replicatedValue = false;
                if (complete && replicatedValue) {
                    System.out.println("RaftMigrationCoordinatorFailoverTest passed: elected leader resumed durable migration intent");
                    return;
                }
                Thread.sleep(25);
            }
            throw new AssertionError("New coordinator did not finish the committed in-flight migration: "
                    + java.util.Arrays.deepToString(java.util.Arrays.stream(groups).map(group -> java.util.Arrays.stream(group)
                    .map(node -> node == null ? "closed" : node.isLeader() + "/" + node.getMigrationIntent() + "/"
                            + node.isMaintenanceFrozen() + "/" + node.getOwnershipEpoch() + "/" + node.getOwnershipGroups())
                    .toArray()).toArray()));
        } finally {
            for (RaftGroup[] group : groups) for (RaftGroup node : group)
                if (node != null) try { node.close(); } catch (Exception ignored) { }
            for (RedisStore[] group : stores) for (RedisStore store : group)
                if (store != null) try { store.close(); } catch (Exception ignored) { }
            try (var paths = Files.walk(root)) {
                paths.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
                    try { Files.deleteIfExists(path); } catch (Exception ignored) { }
                });
            }
        }
    }

    @Test
    void resumesCommittedIntentAfterCoordinatorFailure() throws Exception { main(new String[0]); }

    private static int awaitLeader(RaftGroup[] group) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(12);
        while (System.nanoTime() < deadline) {
            for (int i = 0; i < group.length; i++) if (group[i].isLeader()) return i;
            Thread.sleep(20);
        }
        throw new AssertionError("Raft group did not elect a leader");
    }
}
