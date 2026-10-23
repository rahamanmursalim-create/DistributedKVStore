package com.codecrafters.redis.cluster;

import java.util.Map;

public final class ClusterRuntimeConfigTest {
    public static void main(String[] args) {
        assert ClusterRuntimeConfig.fromEnvironment(Map.of()).isEmpty();
        ClusterRuntimeConfig config = ClusterRuntimeConfig.fromEnvironment(Map.of(
                "KV_NODE_ID", "node-a",
                "KV_RAFT_GROUP_ID", "orders-1",
                "KV_RAFT_GROUPS", "orders-1,orders-2",
                "KV_RAFT_BIND_HOST", "0.0.0.0",
                "KV_RAFT_PORT", "7101",
                "KV_RAFT_PEERS", "node-a=10.0.0.1:7101,node-b=10.0.0.2:7102,node-c=[2001:db8::3]:7103",
                "KV_FOLLOWER_READS", "true",
                "KV_FOLLOWER_MAX_COMMIT_LAG", "3")).orElseThrow();
        assert "node-a".equals(config.nodeId());
        assert "orders-1".equals(config.groupId());
        assert config.bindPort() == 7101;
        assert config.groupIds().equals(java.util.List.of("orders-1", "orders-2"));
        assert config.bindPort(1) == 7102;
        assert config.selfEndpoint(0).host().equals("10.0.0.1");
        assert config.selfEndpoint(1).port() == 7102;
        assert config.peers().size() == 2;
        assert config.peers().get("node-c").host().equals("2001:db8::3");
        assert config.peers(1).get("node-c").port() == 7104;
        assert config.followerReads();
        assert config.maxFollowerCommitLag() == 3;
        rejects(Map.of("KV_NODE_ID", "node-a"));
        rejects(Map.of("KV_NODE_ID", "node-a", "KV_RAFT_PEERS", "node-b=host:7002"));
        rejects(Map.of("KV_NODE_ID", "node-a", "KV_RAFT_PEERS", "node-a=host:7001,node-a=host:7001,node-b=host:7002"));
        rejects(Map.of("KV_NODE_ID", "node-a", "KV_RAFT_PORT", "7001",
                "KV_RAFT_PEERS", "node-a=host:7002,node-b=host:7003"));
        System.out.println("ClusterRuntimeConfigTest passed");
    }

    private static void rejects(Map<String, String> environment) {
        boolean rejected = false;
        try { ClusterRuntimeConfig.fromEnvironment(environment); }
        catch (IllegalArgumentException expected) { rejected = true; }
        assert rejected : "Expected invalid cluster configuration to be rejected";
    }
}
