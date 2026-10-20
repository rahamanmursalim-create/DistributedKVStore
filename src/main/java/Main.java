
import com.codecrafters.redis.Server;
import com.codecrafters.redis.storage.RedisStore;
import com.codecrafters.redis.web.WebServer;
import com.codecrafters.redis.raft.RaftGroup;
import com.codecrafters.redis.raft.MultiRaftStore;
import com.codecrafters.redis.cluster.ClusterRuntimeConfig;
import com.codecrafters.redis.sharding.ShardedStore;
import com.codecrafters.redis.sharding.ShardAdminService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        int httpPort = readPort("PORT", 8080);
        int tcpPort = readPort("TCP_PORT", 6379);

        String dataDir = System.getenv("DATA_DIR");
        Path dataRoot = dataDir != null && !dataDir.isBlank() ? Path.of(dataDir) : Path.of("data");
        RedisStore store = openConfiguredStore(dataRoot.resolve("wal.log"), dataRoot.resolve("rocksdb"), true);
        ShardedStore shardedStore = null;
        ShardAdminService shardAdmin = null;
        Map<String, String> shardStoragePaths = new LinkedHashMap<>();
        MultiRaftStore multiRaftStore = null;
        Map<String, RaftGroup> raftGroups = new LinkedHashMap<>();
        List<RedisStore> raftStores = new ArrayList<>();
        ClusterRuntimeConfig clusterConfig;
        try {
            shardedStore = createShardedStore(store, dataRoot, dataDir, shardStoragePaths);
            String shardAdminToken = System.getenv("KV_SHARD_ADMIN_TOKEN");
            if (shardedStore != null && shardAdminToken != null && !shardAdminToken.isBlank()) {
                String backend = System.getenv().getOrDefault("KV_STORAGE_BACKEND", "wal");
                shardAdmin = new ShardAdminService(shardedStore, dataRoot, shardAdminToken, backend,
                        shardStoragePaths);
            }
            clusterConfig = ClusterRuntimeConfig.fromEnvironment(System.getenv()).orElse(null);
            if (shardedStore != null && clusterConfig != null)
                throw new IllegalArgumentException("KV_SHARD_NODES mode and Raft cluster mode cannot be combined yet");
            if (clusterConfig != null) {
                String root = dataDir == null || dataDir.isBlank() ? "data" : dataDir;
                for (int i = 0; i < clusterConfig.groupIds().size(); i++) {
                    String groupId = clusterConfig.groupIds().get(i);
                    Map<String, java.net.InetSocketAddress> peers = new LinkedHashMap<>();
                    clusterConfig.peers(i).forEach((id, endpoint) -> peers.put(id, endpoint.address()));
                    Path groupDir = Path.of(root, "raft", groupId);
                    RedisStore groupStore = openConfiguredStore(groupDir.resolve("state.wal"),
                            groupDir.resolve("state-rocksdb"), true);
                    raftStores.add(groupStore);
                    RaftGroup group = new RaftGroup(groupId, clusterConfig.nodeId(),
                            new java.net.InetSocketAddress(clusterConfig.bindHost(), clusterConfig.bindPort(i)),
                            peers, groupDir, groupStore);
                    raftGroups.put(groupId, group);
                }
                multiRaftStore = new MultiRaftStore(raftGroups, 128, clusterConfig.followerReads(),
                        clusterConfig.maxFollowerCommitLag());
                for (RaftGroup group : raftGroups.values()) group.start();
                System.out.println("Multi-Raft cluster mode enabled for node " + clusterConfig.nodeId()
                        + " across groups " + clusterConfig.groupIds());
            }
        } catch (IOException | RuntimeException e) {
            closeRuntime(raftGroups, raftStores, multiRaftStore, shardedStore, store);
            System.err.println("Invalid cluster configuration: " + e.getMessage());
            System.exit(1);
            return;
        }
        com.codecrafters.redis.storage.KeyValueAccess access = multiRaftStore != null
                ? multiRaftStore : shardedStore != null ? shardedStore : store;
        Server tcpServer = access == store ? new Server(tcpPort, store) : new Server(tcpPort, store, access);
        WebServer webServer;

        try {
            webServer = new WebServer(store, access, httpPort, null, false, shardAdmin);
        } catch (IOException e) {
            closeRuntime(raftGroups, raftStores, multiRaftStore, shardedStore, store);
            System.err.println("Could not create HTTP server: " + e.getMessage());
            System.exit(1);
            return;
        }

        ShardedStore activeShardedStore = shardedStore;
        MultiRaftStore activeMultiRaftStore = multiRaftStore;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            webServer.close();
            tcpServer.close();
            closeRuntime(raftGroups, raftStores, activeMultiRaftStore, activeShardedStore, store);
        }));

        Thread tcpThread = new Thread(() -> {
            try {
                tcpServer.start();
            } catch (IOException e) {
                System.err.println("TCP server error: " + e.getMessage());
            }
        }, "kv-tcp-server");

        tcpThread.setDaemon(true);
        tcpThread.start();

        webServer.start();
    }

    private static void closeRaftGroups(Map<String, RaftGroup> groups, List<RedisStore> stores) {
        for (RaftGroup group : groups.values()) try { group.close(); } catch (IOException ignored) { }
        closeRaftStores(stores);
    }

    private static void closeRuntime(Map<String, RaftGroup> groups, List<RedisStore> raftStores,
                                     MultiRaftStore multiRaftStore, ShardedStore shardedStore,
                                     RedisStore baseStore) {
        if (multiRaftStore != null) {
            try { multiRaftStore.close(); } catch (RuntimeException e) {
                System.err.println("Raft group shutdown error: " + e.getMessage());
            }
            closeRaftStores(raftStores);
            baseStore.close();
        } else if (shardedStore != null) {
            shardedStore.close();
        } else if (!groups.isEmpty() || !raftStores.isEmpty()) {
            closeRaftGroups(groups, raftStores);
            baseStore.close();
        } else baseStore.close();
    }

    private static void closeRaftStores(List<RedisStore> stores) {
        for (RedisStore raftStore : stores) raftStore.close();
    }

    private static ShardedStore createShardedStore(RedisStore primary, Path dataRoot, String dataDir,
                                                    Map<String, String> storagePaths) throws IOException {
        Path membershipFile = dataRoot.resolve("shard-membership.tsv");
        String configuredNodes = System.getenv("KV_SHARD_NODES");
        List<String> nodeIds = new ArrayList<>();
        if (java.nio.file.Files.exists(membershipFile)) {
            for (String line : java.nio.file.Files.readAllLines(membershipFile)) {
                String[] fields = line.split("\\t", -1);
                if (fields.length != 2 || nodeIds.contains(fields[0]))
                    throw new IllegalArgumentException("Persisted shard membership is malformed");
                String id = safePathComponent(fields[0]);
                ShardAdminService.validateStoragePath(id, fields[1]);
                nodeIds.add(id);
                storagePaths.put(id, fields[1]);
            }
        } else if (configuredNodes != null && !configuredNodes.isBlank()) {
            for (String value : configuredNodes.split(",")) {
                String id = value.trim();
                if (id.isBlank() || nodeIds.contains(id))
                    throw new IllegalArgumentException("KV_SHARD_NODES contains an empty or duplicate node ID");
                safePathComponent(id);
                nodeIds.add(id);
                storagePaths.put(id, nodeIds.size() == 1 ? "legacy" : "shards/" + id);
            }
        }
        if (nodeIds.isEmpty()) return null;
        ShardedStore router = new ShardedStore(readPort("KV_SHARD_VIRTUAL_NODES", 128));
        String root = dataDir == null || dataDir.isBlank() ? "data" : dataDir;
        boolean primaryRegistered = false;
        try {
            for (int i = 0; i < nodeIds.size(); i++) {
                String id = nodeIds.get(i);
                String storagePath = storagePaths.get(id);
                RedisStore shard;
                if ("legacy".equals(storagePath)) {
                    if (primaryRegistered) throw new IllegalArgumentException("Only one shard can use legacy storage");
                    shard = primary;
                    primaryRegistered = true;
                } else {
                    Path shardDir = Path.of(root).resolve(storagePath);
                    shard = openConfiguredStore(shardDir.resolve("wal.log"), shardDir.resolve("rocksdb"), true);
                }
                router.addNode(nodeIds.get(i), shard);
            }
            if (!primaryRegistered) {
                if (primary.size() > 0)
                    throw new IllegalStateException("Legacy store has keys but no shard is assigned to it; recover data before startup");
                primary.close();
            }
            return router;
        } catch (RuntimeException e) {
            try { router.close(); } catch (RuntimeException closeFailure) { e.addSuppressed(closeFailure); }
            if (!primaryRegistered) primary.close();
            throw e;
        }
    }

    private static String safePathComponent(String value) {
        if (!value.matches("[A-Za-z0-9._-]{1,128}") || value.equals(".") || value.equals(".."))
            throw new IllegalArgumentException("Shard node IDs must be safe path components: " + value);
        return value;
    }

    private static RedisStore openConfiguredStore(Path walPath, Path rocksDirectory, boolean cleanup) {
        String backend = System.getenv().getOrDefault("KV_STORAGE_BACKEND", "wal").trim().toLowerCase(java.util.Locale.ROOT);
        return switch (backend) {
            case "wal" -> new RedisStore(walPath, cleanup);
            case "rocksdb" -> RedisStore.usingRocksDB(rocksDirectory, cleanup);
            default -> throw new IllegalArgumentException("KV_STORAGE_BACKEND must be 'wal' or 'rocksdb'");
        };
    }

    private static int readPort(String name, int fallback) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            return fallback;
        }

        try {
            int port = Integer.parseInt(value);
            if (port < 1 || port > 65535) {
                throw new NumberFormatException("Port out of range");
            }
            return port;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Invalid port in environment variable " + name, e);
        }
    }
}
