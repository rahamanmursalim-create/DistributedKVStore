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
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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
            assert coordinator.hasUnresolvedLocalTransactions()
                    : "Coordinator recovery checkpoint disappeared before every participant was confirmed";
            assert leaders.get(groupIds[1]).hasUnresolvedLocalTransactions()
                    : "Prepared participant was missing from its active recovery index";
            assert "committed-b".equals(routed.get(recoveryKeyB));
            assert "committed-a".equals(routed.get(recoveryKeyA));
            assert !coordinator.hasUnresolvedLocalTransactions()
                    : "Coordinator checkpoint was not cleared after participant recovery";
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

            String capacityKeyA = "tx-capacity-a";
            while (!groupIds[0].equals(routed.groupFor(capacityKeyA))) capacityKeyA += "a";
            String capacityKeyB = null;
            for (int i = 0; capacityKeyB == null; i++) {
                String candidate = "x".repeat(1025) + i;
                if (groupIds[1].equals(routed.groupFor(candidate))) capacityKeyB = candidate;
            }
            boolean capacityRejected = false;
            try { routed.setBatch(Map.of(capacityKeyA, "must-abort", capacityKeyB, "invalid-key-capacity")); }
            catch (java.io.UncheckedIOException expected) { capacityRejected = true; }
            assert capacityRejected : "Participant capacity failure was not rejected";
            assert routed.get(capacityKeyA) == null : "Failed distributed transaction became visible";
            routed.set(capacityKeyA, "key-lock-released");
            assert "key-lock-released".equals(routed.get(capacityKeyA))
                    : "Coordinator abort did not release the prepared key lock";

            String deleteKeyA = batch.keySet().stream().skip(1).findFirst().orElseThrow();
            String deleteKeyB = batch.keySet().stream().skip(2).findFirst().orElseThrow();
            int sameGroupDeleteCount = routed.delete(deleteKeyA, deleteKeyB);
            assert sameGroupDeleteCount == 2 : "same group delete count=" + sameGroupDeleteCount;
            assert routed.get(deleteKeyA) == null;

            ExecutorService races = Executors.newFixedThreadPool(2);
            MultiRaftStore concurrentStore = routed;
            try {
                for (int iteration = 0; iteration < 20; iteration++) {
                    String[] crossKeys = keysOnDifferentGroups(concurrentStore, "concurrent-set-" + iteration);
                    String keyA = crossKeys[0], keyB = crossKeys[1];
                    concurrentStore.set(keyA, "initial-a"); concurrentStore.set(keyB, "initial-b");
                    CyclicBarrier start = new CyclicBarrier(2);
                    Future<Integer> delete = races.submit(() -> { start.await(); return concurrentStore.delete(keyA, keyB); });
                    Future<Boolean> write = races.submit(() -> {
                        start.await();
                        try { concurrentStore.set(keyA, "raced-write"); return true; }
                        catch (java.io.UncheckedIOException transactionLockWon) { return false; }
                    });
                    int deleted = delete.get();
                    boolean writeSucceeded = write.get();
                    String finalValue = concurrentStore.get(keyA);
                    assert (deleted == 1 && "raced-write".equals(finalValue))
                            || (deleted == 2 && finalValue == null)
                            : "Cross-group delete count disagreed with concurrent write: count=" + deleted
                                    + ", value=" + finalValue + ", writeSucceeded=" + writeSucceeded;
                    assert concurrentStore.get(keyB) == null;
                }

                for (int iteration = 0; iteration < 20; iteration++) {
                    String[] crossKeys = keysOnDifferentGroups(concurrentStore, "concurrent-delete-" + iteration);
                    String keyA = crossKeys[0], keyB = crossKeys[1];
                    concurrentStore.set(keyA, "initial-a"); concurrentStore.set(keyB, "initial-b");
                    CyclicBarrier start = new CyclicBarrier(2);
                    Future<Integer> crossDelete = races.submit(() -> { start.await(); return concurrentStore.delete(keyA, keyB); });
                    Future<Integer> singleDelete = races.submit(() -> { start.await(); return concurrentStore.delete(keyA); });
                    int totalDeleted = crossDelete.get() + singleDelete.get();
                    assert totalDeleted == 2 : "Concurrent deletes counted a key more or less than once: " + totalDeleted;
                    assert concurrentStore.get(keyA) == null && concurrentStore.get(keyB) == null;
                }
            } finally { races.shutdownNow(); }

            try (com.codecrafters.redis.web.WebServer metricsServer = new com.codecrafters.redis.web.WebServer(
                    stores[0][leaderIndexes[0]], routed, 0, null, false)) {
                metricsServer.start();
                java.net.http.HttpClient http = java.net.http.HttpClient.newHttpClient();
                java.net.URI metricsUri = java.net.URI.create("http://127.0.0.1:" + metricsServer.getPort() + "/api/metrics");
                String metrics = http.send(java.net.http.HttpRequest.newBuilder(metricsUri).GET().build(),
                        java.net.http.HttpResponse.BodyHandlers.ofString()).body();
                assert metrics.contains("kv_raft_replication_lag_entries{group=\"shard-a\"")
                        : "Multi-Raft peer replication lag metric is missing";
                assert metrics.contains("kv_transaction_recovery_failures_total 0")
                        : "Multi-Raft transaction recovery failure counter is missing";
                assert metrics.contains("kv_wal_size_bytes{group=\"shard-a\"}")
                        : "Per-group WAL size metric is missing";
            }
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

    private static String[] keysOnDifferentGroups(MultiRaftStore store, String prefix) {
        String first = prefix + "-a", second = prefix + "-b";
        while (store.groupFor(first).equals(store.groupFor(second))) second += "b";
        return new String[] {first, second};
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
