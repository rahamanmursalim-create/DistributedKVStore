package com.codecrafters.redis.raft;

import com.codecrafters.redis.storage.RedisStore;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Tests active Raft snapshot transfer, log compaction, follower restart and suffix replay. */
public final class RaftSnapshotInstallTest {
    public static void main(String[] args) throws Exception {
        Path root = Files.createTempDirectory("raft-snapshot-install");
        Path[] dirs = {root.resolve("n1"), root.resolve("n2"), root.resolve("n3")};
        Path[] wals = {root.resolve("n1.wal"), root.resolve("n2.wal"), root.resolve("n3.wal")};
        RedisStore[] stores = {new RedisStore(wals[0], false), new RedisStore(wals[1], false), new RedisStore(wals[2], false)};
        RaftGroup[] nodes = new RaftGroup[3]; int[] ports = reservePorts();
        try {
            for (int i = 0; i < 3; i++) nodes[i] = node(i, dirs, stores, ports);
            for (RaftGroup node : nodes) node.start();
            RaftGroup leader = awaitLeader(nodes);
            int leaderIndex = java.util.Arrays.asList(nodes).indexOf(leader);
            int lagging = (leaderIndex + 1) % 3;
            nodes[lagging].close(); nodes[lagging] = null;
            for (int i = 0; i < 12; i++) leader.put("key-" + i, ("value-" + i).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            long checkpoint = leader.createSnapshot();
            assert checkpoint == leader.getLogBaseIndex() && checkpoint > 1;

            nodes[lagging] = node(lagging, dirs, stores, ports); nodes[lagging].start();
            leader.put("suffix", "suffix-value".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            assert nodes[lagging].getLogBaseIndex() == checkpoint;
            assert "value-11".equals(stores[lagging].get("key-11"));
            assert "suffix-value".equals(stores[lagging].get("suffix"));

            nodes[lagging].close(); nodes[lagging] = null; stores[lagging].close();
            stores[lagging] = new RedisStore(wals[lagging], false);
            nodes[lagging] = node(lagging, dirs, stores, ports); nodes[lagging].start();
            assert nodes[lagging].getLogBaseIndex() == checkpoint;
            assert "value-11".equals(stores[lagging].get("key-11"));
            assert "suffix-value".equals(stores[lagging].get("suffix"));
            System.out.println("RaftSnapshotInstallTest passed: active checkpoint, lagging follower install, WAL restart recovery");
        } finally {
            for (RaftGroup node : nodes) if (node != null) node.close();
            for (RedisStore store : stores) if (store != null) store.close();
            try (var paths = Files.walk(root)) {
                for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                    Files.deleteIfExists(path);
                }
            }
        }
    }

    private static RaftGroup node(int index, Path[] dirs, RedisStore[] stores, int[] ports) throws Exception {
        Map<String, InetSocketAddress> peers = new LinkedHashMap<>();
        for (int i = 0; i < 3; i++) if (i != index) peers.put("n" + (i + 1), new InetSocketAddress("127.0.0.1", ports[i]));
        return new RaftGroup("snapshot", "n" + (index + 1), new InetSocketAddress("127.0.0.1", ports[index]), peers, dirs[index], stores[index]);
    }
    private static int[] reservePorts() throws Exception {
        int[] ports = new int[3];
        List<ServerSocket> reservations = new ArrayList<>();
        try {
            for (int i = 0; i < ports.length; i++) {
                ServerSocket socket = new ServerSocket(0);
                reservations.add(socket);
                ports[i] = socket.getLocalPort();
            }
        } finally {
            for (ServerSocket socket : reservations) socket.close();
        }
        return ports;
    }

    private static RaftGroup awaitLeader(RaftGroup[] nodes) throws Exception {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            for (int i = 0; i < nodes.length; i++) {
                RaftGroup candidate = nodes[i];
                if (!readyLeader(nodes, i, candidate)) continue;
                // Require one complete heartbeat interval with the same
                // committed leader still recognized by a voting majority.
                Thread.sleep(100);
                if (readyLeader(nodes, i, candidate)) return candidate;
            }
            Thread.sleep(20);
        }
        throw new AssertionError("No stable, quorum-confirmed Raft leader elected: "
                + java.util.Arrays.stream(nodes).map(node -> node == null ? "closed"
                : "leader=" + node.getLeaderId() + ",term=" + node.getTerm()
                + ",commit=" + node.getCommitIndex()).toList());
    }

    private static boolean readyLeader(RaftGroup[] nodes, int leaderIndex, RaftGroup candidate) {
        if (candidate == null || !candidate.isLeader() || candidate.getCommitIndex() == 0
                || !("n" + (leaderIndex + 1)).equals(candidate.getLeaderId())) return false;
        int recognizingVoters = 0;
        for (RaftGroup node : nodes) {
            if (node != null && candidate.getLeaderId().equals(node.getLeaderId())) recognizingVoters++;
        }
        return recognizingVoters >= 2;
    }
}
