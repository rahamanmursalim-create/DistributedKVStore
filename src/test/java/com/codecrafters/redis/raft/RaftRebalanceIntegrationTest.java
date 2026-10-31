package com.codecrafters.redis.raft;

import org.junit.jupiter.api.Test;

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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
            Map<String, String> values = new LinkedHashMap<>();
            int failedReplica = -1;
            for (int candidate = 0; candidate < nodeIds.length && failedReplica < 0; candidate++) {
                boolean followerForEveryGroup = true;
                for (int group = 0; group < groupIds.length; group++)
                    if (leaders.get(groupIds[group]) == groups[group][candidate]) followerForEveryGroup = false;
                if (followerForEveryGroup) failedReplica = candidate;
            }
            if (failedReplica < 0) throw new AssertionError("Could not select one follower replica for every group");
            final int replicaToFail = failedReplica;
            Map<String, RaftGroup> coordinatorGroups = new LinkedHashMap<>();
            int coordinatorNode = -1;
            for (int candidate = 0; candidate < nodeIds.length; candidate++)
                if (groups[0][candidate] == leaders.get(groupIds[0])) coordinatorNode = candidate;
            for (int group = 0; group < groupIds.length; group++) {
                coordinatorGroups.put(groupIds[group], groups[group][coordinatorNode]);
            }
            MultiRaftStore routed = new MultiRaftStore(coordinatorGroups, 64, false);
            MultiRaftStore staleRouter = new MultiRaftStore(coordinatorGroups, 64, false);
            for (int candidate = 0; values.size() < 80; candidate++) {
                String key = "rebalance-" + candidate;
                if ("b".equals(routed.groupFor(key))) values.put(key, "value-" + candidate);
            }
            routed.setBatch(values);
            String expiringKey = values.keySet().iterator().next();
            routed.set(expiringKey, "ttl-value", 120_000);

            AtomicBoolean writing = new AtomicBoolean(true);
            AtomicReference<Throwable> writerFailure = new AtomicReference<>();
            CountDownLatch migrationStarted = new CountDownLatch(1);
            Thread writer = new Thread(() -> {
                try {
                    migrationStarted.await();
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    boolean frozen = false;
                    while (System.nanoTime() < deadline && !frozen) {
                        for (RaftGroup[] group : groups) for (RaftGroup node : group)
                            if (node != null && node.isMaintenanceFrozen()) frozen = true;
                        if (!frozen) Thread.sleep(1);
                    }
                    if (!frozen) throw new AssertionError("Concurrent writer did not overlap migration freeze");
                    for (int sequence = 0; sequence < 100 && writing.get(); sequence++) {
                        routed.set("during-rebalance-" + sequence, "value-" + sequence);
                    }
                } catch (Throwable failure) { writerFailure.set(failure); }
            }, "rebalance-concurrent-writer");
            writer.start();
            Thread failedNode = new Thread(() -> {
                try {
                    migrationStarted.await();
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    boolean frozen = false;
                    while (System.nanoTime() < deadline && !frozen) {
                        for (RaftGroup[] group : groups) for (RaftGroup node : group)
                            if (node != null && node.isMaintenanceFrozen()) frozen = true;
                        if (!frozen) Thread.sleep(1);
                    }
                    if (!frozen) throw new AssertionError("Migration never reached its quorum-frozen phase");
                    for (int group = 0; group < groupIds.length; group++) {
                        groups[group][replicaToFail].close();
                        groups[group][replicaToFail] = null;
                    }
                } catch (Throwable failure) { writerFailure.compareAndSet(null, failure); }
            }, "rebalance-replica-failure");
            failedNode.start();
            migrationStarted.countDown();
            routed.rebalanceGroups(java.util.List.of("a"));
            writer.join(TimeUnit.SECONDS.toMillis(10));
            writing.set(false);
            failedNode.join(TimeUnit.SECONDS.toMillis(10));
            if (writer.isAlive() || failedNode.isAlive()) throw new AssertionError("Concurrent migration fault workers did not finish");
            if (writerFailure.get() != null) throw new AssertionError("Concurrent write/node failure overlapped migration", writerFailure.get());
            for (int i = 0; i < 100; i++)
                if (!("value-" + i).equals(routed.get("during-rebalance-" + i)))
                    throw new AssertionError("Write queued during migration was lost: " + i);
            for (int group = 0; group < groupIds.length; group++) {
                Map<String, InetSocketAddress> peers = new LinkedHashMap<>();
                for (int peer = 0; peer < nodeIds.length; peer++) if (peer != replicaToFail)
                    peers.put(nodeIds[peer], new InetSocketAddress("127.0.0.1", ports[group][peer]));
                Path directory = root.resolve(groupIds[group] + "-" + nodeIds[replicaToFail]);
                groups[group][replicaToFail] = new RaftGroup(groupIds[group], nodeIds[replicaToFail],
                        new InetSocketAddress("127.0.0.1", ports[group][replicaToFail]), peers,
                        directory, stores[group][replicaToFail]);
                groups[group][replicaToFail].start();
            }
            awaitReplicas(groups, stores, 10_000);

            if (!"a".equals(staleRouter.groupFor(expiringKey)))
                throw new AssertionError("A live router did not adopt the quorum-committed ownership manifest");
            long routerDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
            boolean routerWriteDone = false;
            while (System.nanoTime() < routerDeadline && !routerWriteDone) {
                try { staleRouter.set("router-refresh", "adopted"); routerWriteDone = true; }
                catch (java.io.UncheckedIOException transientElection) { Thread.sleep(25); }
            }
            if (!routerWriteDone || !"adopted".equals(staleRouter.get("router-refresh")))
                throw new AssertionError("A live router could not read/write after adopting new ownership");
            MultiRaftStore restoredRouter = new MultiRaftStore(coordinatorGroups, 64, false);
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
            System.out.println("RaftRebalanceIntegrationTest passed: concurrent writes, follower failure/restart, moved=80, replicas=3, ttl=preserved");
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

    private static void awaitReplicas(RaftGroup[][] groups, RedisStore[][] stores, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            boolean caughtUp = true;
            for (int group = 0; group < groups.length; group++) {
                long commit = java.util.Arrays.stream(groups[group]).mapToLong(RaftGroup::getCommitIndex).max().orElse(0);
                for (int node = 0; node < groups[group].length; node++) {
                    if (groups[group][node] == null || groups[group][node].getCommitIndex() < commit) caughtUp = false;
                }
            }
            if (caughtUp) return;
            Thread.sleep(25);
        }
        throw new AssertionError("Restarted replica did not catch up to the committed log");
    }

    private static void deleteTree(Path root) throws Exception {
        if (!Files.exists(root)) return;
        try (var paths = Files.walk(root)) {
            for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) Files.deleteIfExists(path);
        }
    }

    @Test
    void runsExistingScenarioUnderJUnit() throws Exception {
        main(new String[0]);
    }
}
