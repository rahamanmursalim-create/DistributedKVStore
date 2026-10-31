package com.codecrafters.redis.raft;

import org.junit.jupiter.api.Test;

import com.codecrafters.redis.storage.RedisStore;
import com.codecrafters.redis.command.Command;
import com.codecrafters.redis.command.CommandHandler;
import com.codecrafters.redis.protocol.RespType;
import com.codecrafters.redis.protocol.RespValue;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public final class RaftGroupConsensusTest {
    public static void main(String[] args) throws Exception {
        Path root = Files.createTempDirectory("raft-group-test");
        Path[] dirs = {root.resolve("n1"), root.resolve("n2"), root.resolve("n3")};
        RedisStore[] stores = {
                new RedisStore(root.resolve("n1.wal"), false),
                new RedisStore(root.resolve("n2.wal"), false),
                new RedisStore(root.resolve("n3.wal"), false)
        };
        RaftGroup[] nodes = new RaftGroup[3];
        int[] ports = new int[3];
        int firstLeader = -1;
        try {
            int p1 = ports[0] = availablePort(), p2 = ports[1] = availablePort(), p3 = ports[2] = availablePort();
            nodes[0] = new RaftGroup("g1", "n1", new InetSocketAddress("127.0.0.1", p1), Map.of(
                    "n2", new InetSocketAddress("127.0.0.1", p2), "n3", new InetSocketAddress("127.0.0.1", p3)), dirs[0], stores[0]);
            nodes[1] = new RaftGroup("g1", "n2", new InetSocketAddress("127.0.0.1", p2), Map.of(
                    "n1", new InetSocketAddress("127.0.0.1", p1), "n3", new InetSocketAddress("127.0.0.1", p3)), dirs[1], stores[1]);
            nodes[2] = new RaftGroup("g1", "n3", new InetSocketAddress("127.0.0.1", p3), Map.of(
                    "n1", new InetSocketAddress("127.0.0.1", p1), "n2", new InetSocketAddress("127.0.0.1", p2)), dirs[2], stores[2]);
            nodes[0].start(); nodes[1].start(); nodes[2].start();
            firstLeader = awaitLeader(nodes, -1);
            assert firstLeader >= 0;
            RaftGroup leader = nodes[firstLeader];
            assert leader.put("alpha", "replicated".getBytes(java.nio.charset.StandardCharsets.UTF_8)) >= 2;
            assert "replicated".equals(stores[0].get("alpha"));
            assert "replicated".equals(stores[1].get("alpha"));
            assert "replicated".equals(stores[2].get("alpha"));
            assert leader.readIndex() == leader.getCommitIndex();
            int boundedFollower = (firstLeader + 1) % 3;
            long leaderDiscoveryDeadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(3);
            while (nodes[boundedFollower].getLeaderId() == null && System.nanoTime() < leaderDiscoveryDeadline)
                Thread.sleep(10);
            assert nodes[boundedFollower].getLeaderId() != null;
            assert "replicated".equals(new String(nodes[boundedFollower].getBoundedStalenessRead("alpha", 0), java.nio.charset.StandardCharsets.UTF_8));
            assert "replicated".equals(new String(leader.getWithReadIndex("alpha"), java.nio.charset.StandardCharsets.UTF_8));
            CommandHandler commands = new CommandHandler(stores[firstLeader], null, leader, false);
            assert "OK".equals(commands.execute(new Command("SET", java.util.List.of("via-command", "yes"))).getStringValue());
            assert "yes".equals(commands.execute(new Command("GET", java.util.List.of("via-command"))).getStringValue());
            RespValue deleted = commands.execute(new Command("DEL", java.util.List.of("via-command")));
            assert deleted.getType() == RespType.INTEGER && Long.valueOf(1).equals(deleted.getIntegerValue());
            assert commands.execute(new Command("GET", java.util.List.of("via-command"))).getType() == RespType.NULL_BULK_STRING;
            assert "OK".equals(commands.execute(new Command("SET", java.util.List.of("del-batch-a", "1"))).getStringValue());
            assert "OK".equals(commands.execute(new Command("SET", java.util.List.of("del-batch-b", "2"))).getStringValue());
            RespValue batchDeleted = commands.execute(new Command("DEL", java.util.List.of("del-batch-a", "del-batch-b")));
            assert batchDeleted.getType() == RespType.INTEGER && Long.valueOf(2).equals(batchDeleted.getIntegerValue());
            assert stores[firstLeader].get("del-batch-a") == null && stores[firstLeader].get("del-batch-b") == null;
            assert "OK".equals(commands.execute(new Command("SET", java.util.List.of("del-duplicate", "1"))).getStringValue());
            RespValue duplicateDeleted = commands.execute(new Command("DEL", java.util.List.of("del-duplicate", "del-duplicate")));
            assert duplicateDeleted.getType() == RespType.INTEGER && Long.valueOf(1).equals(duplicateDeleted.getIntegerValue());
            assert "OK".equals(commands.execute(new Command("SET", java.util.List.of("ttl-command", "expires-later", "PX", "10000"))).getStringValue());
            assert "expires-later".equals(commands.execute(new Command("GET", java.util.List.of("ttl-command"))).getStringValue());
            int followerIndex = (firstLeader + 1) % 3;
            CommandHandler followerCommands = new CommandHandler(stores[followerIndex], null, nodes[followerIndex], false);
            assert "OK".equals(followerCommands.execute(new Command("SET", java.util.List.of("forwarded", "through-leader"))).getStringValue());
            assert "through-leader".equals(followerCommands.execute(new Command("GET", java.util.List.of("forwarded"))).getStringValue());
            assert followerCommands.execute(new Command("KEYS", java.util.List.of("forwarded")))
                    .getArrayValue().size() == 1;
            for (RedisStore replica : stores) assert "through-leader".equals(replica.get("forwarded"));
            nodes[firstLeader].close(); nodes[firstLeader] = null;
            int newLeaderIndex = awaitLeader(nodes, firstLeader);
            if (newLeaderIndex < 0 || newLeaderIndex == firstLeader) {
                throw new AssertionError("No failover leader: " + java.util.Arrays.toString(java.util.Arrays.stream(nodes)
                        .map(node -> node == null ? "closed" : "leader=" + node.getLeaderId() + ",term=" + node.getTerm()
                                + ",commit=" + node.getCommitIndex() + ",log=" + node.getLastLogIndex() + "/" + node.getLastLogTerm()
                                + ",base=" + node.getLogBaseIndex() + ",voters=" + node.getVoterIds()).toArray()));
            }
            RaftGroup replacementLeader = nodes[newLeaderIndex];
            assert replacementLeader.put("after-failover", "ok".getBytes(java.nio.charset.StandardCharsets.UTF_8))
                    > replacementLeader.getCommitIndex() - 1;
            int recoveryIndex = -1;
            for (int i = 0; i < 3; i++) if (i != firstLeader && i != newLeaderIndex) recoveryIndex = i;
            assert recoveryIndex >= 0 && "ok".equals(stores[recoveryIndex].get("after-failover"));
            long durableCommitIndex = nodes[recoveryIndex].getCommitIndex();
            nodes[recoveryIndex].close(); nodes[recoveryIndex] = null; stores[recoveryIndex].close();
            stores[recoveryIndex] = new RedisStore(root.resolve("n" + (recoveryIndex + 1) + ".wal"), false);
            try (RaftGroup recovered = new RaftGroup("g1", "n" + (recoveryIndex + 1),
                    new InetSocketAddress("127.0.0.1", ports[recoveryIndex]), Map.of(), dirs[recoveryIndex], stores[recoveryIndex])) {
                assert recovered.getCommitIndex() == durableCommitIndex;
                assert "ok".equals(new String(recovered.get("after-failover"), java.nio.charset.StandardCharsets.UTF_8));
            }
            System.out.println("RaftGroupConsensusTest passed");
        } finally {
            for (RaftGroup node : nodes) if (node != null) node.close();
            for (RedisStore store : stores) store.close();
        }
    }

    private static int availablePort() throws Exception {
        try (java.net.ServerSocket socket = new java.net.ServerSocket(0)) { return socket.getLocalPort(); }
    }

    private static int awaitLeader(RaftGroup[] nodes, int excluded) throws InterruptedException {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(12);
        while (System.nanoTime() < deadline) {
            for (int i = 0; i < nodes.length; i++)
                if (i != excluded && nodes[i] != null && nodes[i].isLeader()) return i;
            Thread.sleep(25);
        }
        return -1;
    }

    @Test
    void runsExistingScenarioUnderJUnit() throws Exception {
        main(new String[0]);
    }
}
