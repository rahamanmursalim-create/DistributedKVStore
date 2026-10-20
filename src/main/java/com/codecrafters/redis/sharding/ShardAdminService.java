package com.codecrafters.redis.sharding;

import com.codecrafters.redis.storage.RedisStore;

import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Locale;
import java.util.Objects;

/** Authenticated runtime membership operations for a local {@link ShardedStore}. */
public final class ShardAdminService {
    private final ShardedStore store;
    private final Path dataRoot;
    private final Path membershipFile;
    private final Map<String, String> storagePaths = new LinkedHashMap<>();
    private final String bearerToken;
    private final String backend;

    public ShardAdminService(ShardedStore store, Path dataRoot, String bearerToken, String backend) throws IOException {
        this(store, dataRoot, bearerToken, backend, defaultStoragePaths(store.nodeIds()));
    }

    public ShardAdminService(ShardedStore store, Path dataRoot, String bearerToken, String backend,
                             Map<String, String> storagePaths) throws IOException {
        this.store = Objects.requireNonNull(store, "store");
        this.dataRoot = Objects.requireNonNull(dataRoot, "dataRoot").toAbsolutePath().normalize();
        this.membershipFile = this.dataRoot.resolve("shard-membership.tsv");
        if (bearerToken == null || bearerToken.isBlank())
            throw new IllegalArgumentException("A non-empty shard admin token is required");
        this.bearerToken = bearerToken;
        this.backend = Objects.requireNonNull(backend, "backend").trim().toLowerCase(Locale.ROOT);
        if (!this.backend.equals("wal") && !this.backend.equals("rocksdb"))
            throw new IllegalArgumentException("Storage backend must be 'wal' or 'rocksdb'");
        this.storagePaths.putAll(Objects.requireNonNull(storagePaths, "storagePaths"));
        if (!this.storagePaths.keySet().containsAll(store.nodeIds()))
            throw new IllegalArgumentException("Every shard must have a persisted storage path");
        if (!Files.exists(membershipFile)) persistMembership(store.nodeIds(), this.storagePaths);
    }

    public List<String> nodeIds() { return store.nodeIds(); }

    public boolean isAuthorized(String authorization) {
        if (authorization == null || !authorization.startsWith("Bearer ")) return false;
        byte[] supplied = authorization.substring(7).getBytes(StandardCharsets.UTF_8);
        byte[] expected = bearerToken.getBytes(StandardCharsets.UTF_8);
        return MessageDigest.isEqual(expected, supplied);
    }

    public void addNode(String nodeId) {
        validateNodeId(nodeId);
        List<String> current = store.nodeIds();
        if (current.contains(nodeId)) throw new IllegalArgumentException("Node already exists: " + nodeId);
        Path nodeDirectory = dataRoot.resolve("shards").resolve(nodeId).normalize();
        if (!nodeDirectory.startsWith(dataRoot.resolve("shards").normalize()))
            throw new IllegalArgumentException("Invalid shard node ID");
        RedisStore nodeStore = backend.equals("rocksdb")
                ? RedisStore.usingRocksDB(nodeDirectory.resolve("rocksdb"), true)
                : new RedisStore(nodeDirectory.resolve("wal.log"), true);
        boolean added = false;
        try {
            List<String> desired = new java.util.ArrayList<>(current);
            desired.add(nodeId);
            Map<String, String> desiredPaths = new LinkedHashMap<>(storagePaths);
            desiredPaths.put(nodeId, "shards/" + nodeId);
            persistMembership(desired, desiredPaths);
            store.addNode(nodeId, nodeStore);
            storagePaths.clear();
            storagePaths.putAll(desiredPaths);
            added = true;
        } catch (RuntimeException failure) {
            try { persistMembership(current, storagePaths); }
            catch (RuntimeException rollbackFailure) { failure.addSuppressed(rollbackFailure); }
            throw failure;
        } finally {
            if (!added) nodeStore.close();
        }
    }

    public void removeNode(String nodeId) {
        validateNodeId(nodeId);
        if (!store.nodeIds().contains(nodeId)) throw new IllegalArgumentException("Unknown shard: " + nodeId);
        store.removeNode(nodeId);
        Map<String, String> desiredPaths = new LinkedHashMap<>(storagePaths);
        desiredPaths.remove(nodeId);
        persistMembership(store.nodeIds(), desiredPaths);
        storagePaths.clear();
        storagePaths.putAll(desiredPaths);
    }

    private void persistMembership(List<String> nodeIds, Map<String, String> paths) {
        try {
            Files.createDirectories(dataRoot);
            Path temporary = membershipFile.resolveSibling(membershipFile.getFileName() + ".tmp");
            StringBuilder contents = new StringBuilder();
            for (String nodeId : nodeIds) {
                String storagePath = paths.get(nodeId);
                validateStoragePath(nodeId, storagePath);
                contents.append(nodeId).append('\t').append(storagePath).append(System.lineSeparator());
            }
            Files.writeString(temporary, contents, StandardCharsets.UTF_8);
            try {
                Files.move(temporary, membershipFile, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (java.nio.file.AtomicMoveNotSupportedException e) {
                Files.move(temporary, membershipFile, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Could not persist shard membership", e);
        }
    }

    private static Map<String, String> defaultStoragePaths(List<String> nodeIds) {
        Map<String, String> result = new LinkedHashMap<>();
        for (int i = 0; i < nodeIds.size(); i++)
            result.put(nodeIds.get(i), i == 0 ? "legacy" : "shards/" + nodeIds.get(i));
        return result;
    }

    public static void validateStoragePath(String nodeId, String storagePath) {
        validateNodeId(nodeId);
        if (!("legacy".equals(storagePath) || ("shards/" + nodeId).equals(storagePath)))
            throw new IllegalArgumentException("Invalid storage path for shard " + nodeId);
    }

    private static void validateNodeId(String nodeId) {
        if (nodeId == null || !nodeId.matches("[A-Za-z0-9._-]{1,128}")
                || nodeId.equals(".") || nodeId.equals(".."))
            throw new IllegalArgumentException("Shard IDs must contain only letters, digits, '.', '_' or '-'");
    }
}
