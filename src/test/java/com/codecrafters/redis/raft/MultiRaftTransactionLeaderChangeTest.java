package com.codecrafters.redis.raft;

import com.codecrafters.redis.storage.RedisStore;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Deterministic local failure injection while a participant transaction is prepared. */
final class MultiRaftTransactionLeaderChangeTest {
    @Test
    void participantLeaderChangePreservesPreparedStateAndCommittedDecision() throws Exception {
        Path root = Files.createTempDirectory("multiraft-transaction-leader-change-");
        int coordinatorPort = reservePort();
        int[] participantPorts = reservePorts(3);
        RedisStore coordinatorStore = new RedisStore(root.resolve("coordinator.wal"), false);
        RedisStore[] stores = new RedisStore[3];
        RaftGroup coordinator = new RaftGroup("coordinator", "c1",
                new InetSocketAddress("127.0.0.1", coordinatorPort), Map.of(), root.resolve("coordinator"), coordinatorStore);
        RaftGroup[] participants = new RaftGroup[3];
        MultiRaftStore router = null;
        try {
            for (int i = 0; i < participants.length; i++) {
                stores[i] = new RedisStore(root.resolve("participant-" + i + ".wal"), false);
                participants[i] = participant(i, root, participantPorts, stores[i]);
            }
            coordinator.start();
            for (RaftGroup participant : participants) participant.start();
            RaftGroup participantLeader = awaitLeader(participants, 15);
            int stoppedIndex = java.util.Arrays.asList(participants).indexOf(participantLeader);
            long oldTerm = participantLeader.getTerm();

            String id = "leader-change-commit", digest = "digest-" + id;
            String keyA = "leader-key-a", keyB = "leader-key-b";
            List<String> participantIds = List.of("coordinator", "participant");
            coordinator.beginTransaction(id, digest, participantIds);
            coordinator.prepareTransaction(id, digest, participantIds, Map.of(keyA, "committed-a"));
            participantLeader.prepareTransaction(id, digest, participantIds, Map.of(keyB, "committed-b"));
            participantLeader.validatePreparedTransactionCapacity(id, digest);
            assertTrue(participantLeader.hasUnresolvedLocalTransactions());

            // Stop the active leader after the durable prepare entry but before decision.
            participantLeader.close(); participants[stoppedIndex] = null;
            stores[stoppedIndex].close(); stores[stoppedIndex] = null;
            RaftGroup replacement = awaitLeader(participants, 15);
            assertNotSame(participantLeader, replacement);
            assertTrue(replacement.getTerm() > oldTerm, "Participant election did not advance the term");
            RaftGroup.TransactionRecord rebuilt = replacement.transactionWithReadIndex(id);
            assertNotNull(rebuilt, "New leader lost the prepared transaction from committed Raft state");
            assertTrue(rebuilt.prepared());
            replacement.validatePreparedTransactionCapacity(id, digest);

            coordinator.decideTransaction(id, digest, participantIds, true);
            restartParticipant(stoppedIndex, root, participantPorts, stores, participants);
            RaftGroup currentLeader = awaitLeader(participants, 15);
            assertNotNull(currentLeader.transactionWithReadIndex(id));
            assertTrue(currentLeader.hasUnresolvedLocalTransactions());

            Map<String, RaftGroup> groups = new LinkedHashMap<>();
            groups.put("coordinator", coordinator);
            groups.put("participant", currentLeader);
            router = new MultiRaftStore(groups, 64, false);
            router.recoverPendingTransactions();

            assertEquals("committed-a", text(coordinator.getWithReadIndex(keyA)));
            for (RaftGroup participant : participants)
                assertEquals("committed-b", text(participant.getWithReadIndex(keyB)));
            assertFalse(coordinator.hasUnresolvedLocalTransactions(), "Coordinator checkpoint remains after recovery");
            assertFalse(currentLeader.hasUnresolvedLocalTransactions(), "Participant active index remains after recovery");
            currentLeader.put(keyB, "committed-b".getBytes(StandardCharsets.UTF_8));
            System.out.println("MultiRaftTransactionLeaderChangeTest passed: prepared state survived participant election and commit recovery");
        } finally {
            if (router != null) router.close();
            else {
                try { coordinator.close(); } catch (Exception ignored) { }
            }
            for (RaftGroup participant : participants) if (participant != null)
                try { participant.close(); } catch (Exception ignored) { }
            try { coordinatorStore.close(); } catch (Exception ignored) { }
            for (RedisStore store : stores) if (store != null) try { store.close(); } catch (Exception ignored) { }
            try (var paths = Files.walk(root)) {
                for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) Files.deleteIfExists(path);
            }
        }
    }

    private static RaftGroup restartParticipant(int index, Path root, int[] ports,
                                                RedisStore[] stores, RaftGroup[] participants) throws Exception {
        stores[index] = new RedisStore(root.resolve("participant-" + index + ".wal"), false);
        participants[index] = participant(index, root, ports, stores[index]);
        participants[index].start();
        return participants[index];
    }

    private static RaftGroup participant(int index, Path root, int[] ports, RedisStore store) throws Exception {
        Map<String, InetSocketAddress> peers = new LinkedHashMap<>();
        for (int peer = 0; peer < ports.length; peer++) if (peer != index)
            peers.put("p" + peer, new InetSocketAddress("127.0.0.1", ports[peer]));
        return new RaftGroup("participant", "p" + index,
                new InetSocketAddress("127.0.0.1", ports[index]), peers,
                root.resolve("participant-" + index), store);
    }

    private static RaftGroup awaitLeader(RaftGroup[] nodes, int seconds) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        while (System.nanoTime() < deadline) {
            for (int i = 0; i < nodes.length; i++) {
                RaftGroup node = nodes[i];
                if (node != null && node.isLeader() && ("p" + i).equals(node.getLeaderId())) {
                long term = node.getTerm();
                Thread.sleep(100);
                if (node.isLeader() && node.getTerm() == term && recognizedByMajority(node, nodes)) return node;
                }
            }
            Thread.sleep(20);
        }
        throw new AssertionError("Participant group failed to elect a quorum-confirmed leader: "
                + java.util.Arrays.stream(nodes).map(node -> node == null ? "stopped"
                : "leader=" + node.getLeaderId() + ",term=" + node.getTerm()).toList());
    }

    private static boolean recognizedByMajority(RaftGroup leader, RaftGroup[] nodes) {
        int count = 0;
        for (RaftGroup node : nodes) if (node != null && leader.getLeaderId().equals(node.getLeaderId())) count++;
        return count >= 2;
    }

    private static String text(byte[] bytes) { return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8); }

    private static int reservePort() throws Exception { return reservePorts(1)[0]; }
    private static int[] reservePorts(int count) throws Exception {
        List<ServerSocket> sockets = new ArrayList<>(); int[] ports = new int[count];
        try {
            for (int i = 0; i < count; i++) { sockets.add(new ServerSocket(0)); ports[i] = sockets.get(i).getLocalPort(); }
        } finally { for (ServerSocket socket : sockets) socket.close(); }
        return ports;
    }
}
