package com.codecrafters.redis.raft;

import org.junit.jupiter.api.Test;

import com.codecrafters.redis.storage.RedisStore;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/** Exercises learner catch-up and joint-consensus replacement of the current leader. */
public final class RaftDynamicMembershipTest {
    public static void main(String[] args) throws Exception {
        Path root = Files.createTempDirectory("raft-membership-test");
        String[] ids = {"n1", "n2", "n3", "n4"};
        int[] ports = {availablePort(), availablePort(), availablePort(), availablePort()};
        RaftGroup[] nodes = new RaftGroup[4];
        RedisStore[] stores = new RedisStore[4];
        try {
            for (int i = 0; i < 4; i++) {
                Map<String, InetSocketAddress> peers = new LinkedHashMap<>();
                for (int p = 0; p < 3; p++) if (p != i)
                    peers.put(ids[p], new InetSocketAddress("127.0.0.1", ports[p]));
                Path dir = root.resolve(ids[i]);
                stores[i] = new RedisStore(dir.resolve("state.wal"), false);
                nodes[i] = new RaftGroup("membership", ids[i], new InetSocketAddress("127.0.0.1", ports[i]),
                        peers, dir, stores[i], i != 3);
            }
            for (RaftGroup node : nodes) node.start();
            int leaderIndex = awaitLeader(nodes);
            assert leaderIndex >= 0 : "No initial leader elected";
            int removed = leaderIndex;
            RaftGroup leader = nodes[leaderIndex];
            leader.put("learner-catch-up", "before-reconfiguration".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            Map<String, InetSocketAddress> target = new LinkedHashMap<>();
            for (int voter = 0; voter < 4; voter++) if (voter != removed)
                target.put(ids[voter], new InetSocketAddress("127.0.0.1", ports[voter]));
            assert !nodes[3].isLeader() : "Learner elected before joining the voter set";
            leader.reconfigureVoters(target);
            for (RaftGroup node : nodes)
                assert node.getVoterIds().equals(target.keySet()) : "Replica did not apply the final voter set";
            int newLeaderIndex = awaitLeader(nodes, target.keySet());
            assert newLeaderIndex >= 0 && newLeaderIndex != leaderIndex : "Removed leader remained eligible";
            assert "before-reconfiguration".equals(stores[3].get("learner-catch-up"))
                    : "New voter was not caught up before the configuration switch";
            RaftGroup newLeader = nodes[newLeaderIndex];
            long before = newLeader.getCommitIndex();
            newLeader.put("after-membership-change", "committed-by-new-quorum".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            assert newLeader.getCommitIndex() > before;
            assert "committed-by-new-quorum".equals(stores[newLeaderIndex].get("after-membership-change"));
            for (String voter : target.keySet()) {
                int index = java.util.Arrays.asList(ids).indexOf(voter);
                assert "committed-by-new-quorum".equals(stores[index].get("after-membership-change"));
            }
            assert stores[removed].get("after-membership-change") == null
                    : "Removed voter received a post-transition write";
            nodes[3].close(); nodes[3] = null; stores[3].close(); stores[3] = null;
            Path learnerDirectory = root.resolve("n4");
            RedisStore restartedStore = new RedisStore(learnerDirectory.resolve("state.wal"), false);
            Map<String, InetSocketAddress> bootstrapPeers = new LinkedHashMap<>();
            for (int peer = 0; peer < 3; peer++)
                bootstrapPeers.put(ids[peer], new InetSocketAddress("127.0.0.1", ports[peer]));
            try (RaftGroup restarted = new RaftGroup("membership", "n4",
                    new InetSocketAddress("127.0.0.1", ports[3]), bootstrapPeers,
                    learnerDirectory, restartedStore, false)) {
                assert restarted.getVoterIds().equals(target.keySet()) : "Committed voter set was not restored after restart";
                assert "committed-by-new-quorum".equals(restarted.get("after-membership-change") == null ? null
                        : new String(restarted.get("after-membership-change"), java.nio.charset.StandardCharsets.UTF_8));
            } finally { restartedStore.close(); }
            System.out.println("RaftDynamicMembershipTest passed: learner catch-up, joint replacement, leader removal");
        } finally {
            for (RaftGroup node : nodes) if (node != null) try { node.close(); } catch (Exception ignored) { }
            for (RedisStore store : stores) if (store != null) store.close();
        }
    }

    private static int availablePort() throws Exception {
        try (java.net.ServerSocket socket = new java.net.ServerSocket(0)) { return socket.getLocalPort(); }
    }

    private static int awaitLeader(RaftGroup[] nodes) throws InterruptedException {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(8);
        while (System.nanoTime() < deadline) {
            for (int i = 0; i < nodes.length; i++) if (nodes[i].isLeader()) return i;
            Thread.sleep(25);
        }
        return -1;
    }

    private static int awaitLeader(RaftGroup[] nodes, java.util.Set<String> allowed) throws InterruptedException {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(8);
        while (System.nanoTime() < deadline) {
            for (int i = 0; i < nodes.length; i++)
                if (allowed.contains("n" + (i + 1)) && nodes[i].isLeader()) return i;
            Thread.sleep(25);
        }
        return -1;
    }

    @Test
    void runsExistingScenarioUnderJUnit() throws Exception {
        main(new String[0]);
    }
}
