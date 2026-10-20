package com.codecrafters.redis.cluster;

import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Environment configuration for starting one Raft-backed application node. */
public final class ClusterRuntimeConfig {
    public record Endpoint(String host, int port) {
        public Endpoint {
            if (host == null || host.isBlank()) throw new IllegalArgumentException("Endpoint host is required");
            validatePort(port, "endpoint port");
        }
        public InetSocketAddress address() { return new InetSocketAddress(host, port); }
    }

    private final String nodeId, groupId, bindHost;
    private final int bindPort;
    private final List<String> groupIds;
    private final Map<String, Endpoint> peers;
    private final boolean followerReads;
    private final long maxFollowerCommitLag;

    private ClusterRuntimeConfig(String nodeId, String groupId, String bindHost, int bindPort,
                                 List<String> groupIds,
                                 Map<String, Endpoint> peers, boolean followerReads, long maxFollowerCommitLag) {
        this.nodeId = nodeId; this.groupId = groupId; this.bindHost = bindHost;
        this.bindPort = bindPort; this.groupIds = List.copyOf(groupIds);
        this.peers = Map.copyOf(peers); this.followerReads = followerReads;
        this.maxFollowerCommitLag = maxFollowerCommitLag;
    }

    /** Returns empty when cluster mode is disabled (KV_NODE_ID is not set). */
    public static Optional<ClusterRuntimeConfig> fromEnvironment(Map<String, String> environment) {
        Objects.requireNonNull(environment, "environment");
        String nodeId = environment.get("KV_NODE_ID");
        if (nodeId == null || nodeId.isBlank()) return Optional.empty();
        if (nodeId.contains(",") || nodeId.contains("=") || nodeId.contains(" "))
            throw new IllegalArgumentException("KV_NODE_ID contains invalid characters");
        String groupId = environment.getOrDefault("KV_RAFT_GROUP_ID", "default");
        if (!groupId.matches("[A-Za-z0-9._-]{1,128}") || groupId.equals(".") || groupId.equals(".."))
            throw new IllegalArgumentException("KV_RAFT_GROUP_ID must contain only letters, digits, '.', '_' or '-'");
        List<String> groupIds = new ArrayList<>();
        String configuredGroups = environment.get("KV_RAFT_GROUPS");
        if (configuredGroups == null || configuredGroups.isBlank()) groupIds.add(groupId);
        else {
            for (String item : configuredGroups.split(",")) {
                String id = item.trim();
                if (!id.matches("[A-Za-z0-9._-]{1,128}") || id.equals(".") || id.equals("..")
                        || groupIds.contains(id))
                    throw new IllegalArgumentException("KV_RAFT_GROUPS contains an invalid or duplicate group ID: " + id);
                groupIds.add(id);
            }
        }
        String bindHost = environment.getOrDefault("KV_RAFT_BIND_HOST", "0.0.0.0");
        if (bindHost.isBlank()) throw new IllegalArgumentException("KV_RAFT_BIND_HOST is required");
        int bindPort = readPort(environment.getOrDefault("KV_RAFT_PORT", "7000"), "KV_RAFT_PORT");
        String peersText = environment.get("KV_RAFT_PEERS");
        if (peersText == null || peersText.isBlank())
            throw new IllegalArgumentException("KV_RAFT_PEERS is required when KV_NODE_ID enables cluster mode");

        Map<String, Endpoint> members = new LinkedHashMap<>();
        for (String item : peersText.split(",")) {
            String member = item.trim(); int equals = member.indexOf('=');
            if (equals <= 0 || equals == member.length() - 1)
                throw new IllegalArgumentException("Invalid KV_RAFT_PEERS member: " + item);
            String id = member.substring(0, equals).trim();
            if (id.contains(" ") || members.containsKey(id))
                throw new IllegalArgumentException("Invalid or duplicate peer ID: " + id);
            members.put(id, parseEndpoint(member.substring(equals + 1).trim()));
        }
        Endpoint self = members.get(nodeId);
        if (self == null) throw new IllegalArgumentException("KV_RAFT_PEERS must include this node: " + nodeId);
        if (self.port() != bindPort)
            throw new IllegalArgumentException("This node's peer port must match KV_RAFT_PORT");
        if ((long) bindPort + groupIds.size() - 1 > 65535)
            throw new IllegalArgumentException("Raft group port range exceeds 65535");
        for (Endpoint endpoint : members.values())
            if ((long) endpoint.port() + groupIds.size() - 1 > 65535)
                throw new IllegalArgumentException("Peer group port range exceeds 65535");
        members.remove(nodeId);
        if (members.isEmpty()) throw new IllegalArgumentException("Raft cluster must contain at least two members");
        String followerValue = environment.getOrDefault("KV_FOLLOWER_READS", "false");
        if (!followerValue.equalsIgnoreCase("true") && !followerValue.equalsIgnoreCase("false"))
            throw new IllegalArgumentException("KV_FOLLOWER_READS must be true or false");
        long maxFollowerCommitLag;
        try { maxFollowerCommitLag = Long.parseLong(environment.getOrDefault("KV_FOLLOWER_MAX_COMMIT_LAG", "0")); }
        catch (NumberFormatException e) { throw new IllegalArgumentException("KV_FOLLOWER_MAX_COMMIT_LAG must be a non-negative integer", e); }
        if (maxFollowerCommitLag < 0) throw new IllegalArgumentException("KV_FOLLOWER_MAX_COMMIT_LAG must be non-negative");
        return Optional.of(new ClusterRuntimeConfig(nodeId, groupId, bindHost, bindPort, groupIds,
                members, Boolean.parseBoolean(followerValue), maxFollowerCommitLag));
    }

    private static Endpoint parseEndpoint(String value) {
        String host; String portText;
        if (value.startsWith("[")) {
            int end = value.indexOf(']');
            if (end < 0 || end + 1 >= value.length() || value.charAt(end + 1) != ':')
                throw new IllegalArgumentException("Invalid bracketed peer endpoint: " + value);
            host = value.substring(1, end); portText = value.substring(end + 2);
        } else {
            int colon = value.lastIndexOf(':');
            if (colon <= 0 || colon == value.length() - 1)
                throw new IllegalArgumentException("Peer endpoint must be host:port: " + value);
            host = value.substring(0, colon); portText = value.substring(colon + 1);
        }
        return new Endpoint(host, readPort(portText, "peer port"));
    }

    private static int readPort(String text, String name) {
        try {
            int value = Integer.parseInt(text);
            validatePort(value, name);
            return value;
        } catch (NumberFormatException e) { throw new IllegalArgumentException("Invalid " + name + ": " + text, e); }
    }
    private static void validatePort(int value, String name) {
        if (value < 1 || value > 65535) throw new IllegalArgumentException("Invalid " + name + ": " + value);
    }

    public String nodeId() { return nodeId; }
    public String groupId() { return groupId; }
    public List<String> groupIds() { return groupIds; }
    public int bindPort(int groupIndex) { checkGroupIndex(groupIndex); return bindPort + groupIndex; }
    public Map<String, Endpoint> peers(int groupIndex) {
        checkGroupIndex(groupIndex);
        Map<String, Endpoint> result = new LinkedHashMap<>();
        peers.forEach((id, endpoint) -> result.put(id, new Endpoint(endpoint.host(), endpoint.port() + groupIndex)));
        return Map.copyOf(result);
    }
    public String bindHost() { return bindHost; }
    public int bindPort() { return bindPort; }
    public Map<String, Endpoint> peers() { return peers; }
    public boolean followerReads() { return followerReads; }
    public long maxFollowerCommitLag() { return maxFollowerCommitLag; }
    private void checkGroupIndex(int index) {
        if (index < 0 || index >= groupIds.size()) throw new IllegalArgumentException("Invalid Raft group index");
    }
}
