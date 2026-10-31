package com.codecrafters.redis.raft;

import org.junit.jupiter.api.Test;

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
            assert routed.leadershipStatus().keySet().equals(java.util.Set.of(groupIds));
            assert routed.leadershipStatus().values().stream().allMatch(status -> status.localLeader()
                    && status.leaderId() != null && status.commitIndex() >= 0);

            Map<String, String> batch = new LinkedHashMap<>();
            for (int i = 0; batch.size() < 100; i++) {
                String key = "multi-key-" + i;
                if (groupIds[0].equals(routed.groupFor(key))) batch.put(key, "value-" + i);
            }
            routed.setBatch(batch);
            assert routed.keys("multi-key-*").size() == 100;
            for (Map.Entry<String, String> entry : batch.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                assert value.equals(routed.get(key));
                for (int node = 0; node < nodeIds.length; node++) {
                    assert value.equals(stores[0][node].get(key));
                    assert stores[1][node].get(key) == null;
                }
            }
            String otherGroupKey = null;
            for (int i = 0; otherGroupKey == null; i++) {
                String candidate = "cross-group-" + i;
                if (groupIds[1].equals(routed.groupFor(candidate))) otherGroupKey = candidate;
            }
            String crossGroupKey = batch.keySet().iterator().next();
            routed.setBatch(Map.of(crossGroupKey, "atomic-a", otherGroupKey, "atomic-b"));
            assert "atomic-a".equals(routed.get(crossGroupKey));
            assert "atomic-b".equals(routed.get(otherGroupKey));
            for (int node = 0; node < nodeIds.length; node++) {
                assert "atomic-a".equals(stores[0][node].get(crossGroupKey));
                assert "atomic-b".equals(stores[1][node].get(otherGroupKey));
            }
            assert routed.delete(crossGroupKey, otherGroupKey) == 2;
            assert routed.get(crossGroupKey) == null && routed.get(otherGroupKey) == null;

            String recoveryKeyA = "tx-recovery-a";
            String recoveryKeyB = "tx-recovery-b";
            if (!groupIds[0].equals(routed.groupFor(recoveryKeyA))
                    || !groupIds[1].equals(routed.groupFor(recoveryKeyB)))
                throw new AssertionError("Transaction recovery keys did not map to distinct groups");
            String transactionId = "commit-recovery-test";
            String transactionDigest = "digest-commit-recovery-test";
            java.util.List<String> participants = java.util.List.of(groupIds[0], groupIds[1]);
            RaftGroup coordinator = leaders.get(groupIds[0]);
            coordinator.beginTransaction(transactionId, transactionDigest, participants);
            coordinator.prepareTransaction(transactionId, transactionDigest, participants,
                    Map.of(recoveryKeyA, "committed-a"));
            leaders.get(groupIds[1]).prepareTransaction(transactionId, transactionDigest, participants,
                    Map.of(recoveryKeyB, "committed-b"));
            boolean preparedKeyFenced = false;
            try { leaders.get(groupIds[1]).put(recoveryKeyB, "conflicting-write".getBytes(java.nio.charset.StandardCharsets.UTF_8)); }
            catch (java.io.IOException expected) { preparedKeyFenced = true; }
            assert preparedKeyFenced : "A prepared key accepted a competing write";
            coordinator.decideTransaction(transactionId, transactionDigest, participants, true);
            // Simulate a coordinator process crash after applying locally but before applying the other group.
            coordinator.applyCommittedTransaction(transactionId, transactionDigest);
            assert "committed-b".equals(routed.get(recoveryKeyB));
            assert "committed-a".equals(routed.get(recoveryKeyA));
            for (int node = 0; node < nodeIds.length; node++) {
                assert "committed-a".equals(stores[0][node].get(recoveryKeyA));
                assert "committed-b".equals(stores[1][node].get(recoveryKeyB));
            }
            String abortId = "abort-recovery-test", abortDigest = "digest-abort-recovery-test";
            String abortKeyA = "tx-abort-a", abortKeyB = "tx-abort-b";
            coordinator.beginTransaction(abortId, abortDigest, participants);
            coordinator.prepareTransaction(abortId, abortDigest, participants, Map.of(abortKeyA, "never-visible-a"));
            leaders.get(groupIds[1]).prepareTransaction(abortId, abortDigest, participants, Map.of(abortKeyB, "never-visible-b"));
            coordinator.decideTransaction(abortId, abortDigest, participants, false);
            routed.recoverPendingTransactions();
            assert routed.get(abortKeyB) == null;
            leaders.get(groupIds[1]).put(abortKeyB, "after-abort".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            assert "after-abort".equals(routed.get(abortKeyB));
            String deleteKeyA = batch.keySet().stream().skip(1).findFirst().orElseThrow();
            String deleteKeyB = batch.keySet().stream().skip(2).findFirst().orElseThrow();
            int sameGroupDeleteCount = routed.delete(deleteKeyA, deleteKeyB);
            assert sameGroupDeleteCount == 2 : "same group delete count=" + sameGroupDeleteCount;
            assert routed.get(deleteKeyA) == null;
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

    @Test
    void runsExistingScenarioUnderJUnit() throws Exception {
        main(new String[0]);
    }
}
