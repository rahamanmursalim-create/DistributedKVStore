package com.codecrafters.redis.raft;

import com.codecrafters.redis.storage.RedisStore;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Bounds and compatibility coverage for Multi-Raft prepared-batch payloads. */
final class RaftTransactionEncodingTest {
    @Test
    void crossGroupBatchLargerThanModifiedUtfLimitSurvivesSnapshotRestart() throws Exception {
        Path root = Files.createTempDirectory("raft-txn-large-utf8-");
        int[] ports = reservePorts(2);
        String[] ids = {"large-a", "large-b"};
        RedisStore[] stores = new RedisStore[2];
        RaftGroup[] groups = new RaftGroup[2];
        MultiRaftStore router = null;
        try {
            for (int i = 0; i < 2; i++) {
                Path directory = root.resolve(ids[i]);
                stores[i] = new RedisStore(directory.resolve("state.wal"), false);
                groups[i] = singleNodeGroup(ids[i], ports[i], directory, stores[i]);
                groups[i].start(); awaitLeader(groups[i]);
            }
            Map<String, RaftGroup> byId = new LinkedHashMap<>();
            byId.put(ids[0], groups[0]); byId.put(ids[1], groups[1]);
            router = new MultiRaftStore(byId, 64, false);
            String keyA = keyFor(router, ids[0]), keyB = keyFor(router, ids[1]);
            String largeValue = "λ漢😀".repeat(30_000);
            assertTrue(largeValue.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 65_535);
            assertTrue(largeValue.getBytes(java.nio.charset.StandardCharsets.UTF_8).length < 1024 * 1024);

            router.setBatch(Map.of(keyA, "small-value", keyB, largeValue));
            assertEquals("small-value", router.get(keyA));
            assertEquals(largeValue, router.get(keyB));
            RaftGroup largeGroup = groups[1];
            long snapshotIndex = largeGroup.createSnapshot();
            assertTrue(snapshotIndex > 0);
            RaftGroup.TransactionRecord transaction = largeGroup.transactionsWithReadIndex().stream()
                    .filter(record -> record.changes().containsKey(keyB)).findFirst().orElseThrow();
            assertEquals(largeValue, transaction.changes().get(keyB));

            router.close(); router = null;
            for (int i = 0; i < 2; i++) {
                groups[i] = null;
                stores[i].close(); stores[i] = new RedisStore(root.resolve(ids[i]).resolve("state.wal"), false);
                groups[i] = singleNodeGroup(ids[i], ports[i], root.resolve(ids[i]), stores[i]);
                groups[i].start(); awaitLeader(groups[i]);
            }
            assertEquals(largeValue, stores[1].get(keyB));
            RaftGroup.TransactionRecord restored = groups[1].transactionsWithReadIndex().stream()
                    .filter(record -> record.changes().containsKey(keyB)).findFirst().orElseThrow();
            assertEquals(largeValue, restored.changes().get(keyB));
        } finally {
            if (router != null) try { router.close(); } catch (RuntimeException ignored) { }
            for (RaftGroup group : groups) if (group != null) try { group.close(); } catch (Exception ignored) { }
            for (RedisStore store : stores) if (store != null) try { store.close(); } catch (Exception ignored) { }
            deleteTree(root);
        }
    }

    @Test
    void oversizedUtf8FieldIsRejectedBeforeRaftAppend() throws Exception {
        Path root = Files.createTempDirectory("raft-txn-too-large-");
        int port = reservePorts(1)[0];
        RedisStore store = new RedisStore(root.resolve("state.wal"), false);
        RaftGroup group = singleNodeGroup("bounded", port, root.resolve("group"), store);
        try {
            long before = group.getLastLogIndex();
            IOException failure = assertThrows(IOException.class, () -> group.prepareTransaction(
                    "too-large", "digest-too-large", List.of("bounded"),
                    Map.of("key", "x".repeat(16 * 1024 * 1024 + 1))));
            assertTrue(failure.getMessage().contains("exceeds Raft command limit"));
            assertEquals(before, group.getLastLogIndex(), "Rejected payload was appended to the Raft log");
        } finally {
            group.close(); store.close(); deleteTree(root);
        }
    }

    @Test
    void persistedLegacyPrepareEntryStillReplaysAndReleasesItsLock() throws Exception {
        Path root = Files.createTempDirectory("raft-txn-legacy-log-");
        Path directory = root.resolve("group");
        Files.createDirectories(directory);
        int port = reservePorts(1)[0];
        String id = "legacy-log-tx", digest = "legacy-log-digest", key = "legacy-log-key", value = "legacy-log-value";
        try (RaftLog log = new RaftLog(directory.resolve("raft.log"))) {
            log.append(1, legacyPrepareCommand(id, digest, "legacy-log-group", key, value));
        }
        new RaftMetadataStore(directory.resolve("raft.meta")).save(1, null, 1);
        RedisStore store = new RedisStore(directory.resolve("state.wal"), false);
        RaftGroup group = singleNodeGroup("legacy-log-group", port, directory, store);
        try {
            group.start(); awaitLeader(group);
            RaftGroup.TransactionRecord restored = group.transactionWithReadIndex(id);
            assertNotNull(restored);
            assertTrue(restored.prepared());
            assertEquals(value, restored.changes().get(key));
            group.validatePreparedTransactionCapacity(id, digest);
            IOException locked = assertThrows(IOException.class, () -> group.put(key, "conflict".getBytes()));
            assertTrue(locked.getMessage().contains("prepared transaction"));
            group.decideTransaction(id, digest, List.of("legacy-log-group"), false);
            group.abortPreparedTransaction(id, digest);
            group.completeTransaction(id, digest);
            assertFalse(group.hasUnresolvedLocalTransactions());
        } finally {
            group.close(); store.close(); deleteTree(root);
        }
    }

    @Test
    void legacySnapshotWithModifiedUtfTransactionFieldsStillRestores() throws Exception {
        Path root = Files.createTempDirectory("raft-txn-legacy-snapshot-");
        Path directory = root.resolve("group");
        Files.createDirectories(directory);
        int port = reservePorts(1)[0];
        String groupId = "legacy-snapshot-group", nodeId = "n1";
        String id = "legacy-snapshot-tx", digest = "legacy-snapshot-digest";
        String key = "legacy-snapshot-key", value = "legacy-snapshot-value";
        try (RaftLog log = new RaftLog(directory.resolve("raft.log"))) {
            log.append(1, new byte[] {0});
        }
        new RaftMetadataStore(directory.resolve("raft.meta")).save(1, null, 1);
        Files.write(directory.resolve("raft.snapshot"), legacySnapshot(
                1, 1, nodeId, port, id, digest, groupId, key, value));
        RedisStore store = new RedisStore(directory.resolve("state.wal"), false);
        RaftGroup group = singleNodeGroup(groupId, port, directory, store);
        try {
            group.start(); awaitLeader(group);
            RaftGroup.TransactionRecord restored = group.transactionWithReadIndex(id);
            assertNotNull(restored);
            assertTrue(restored.prepared());
            assertEquals(value, restored.changes().get(key));
            group.validatePreparedTransactionCapacity(id, digest);
            IOException locked = assertThrows(IOException.class, () -> group.put(key, "conflict".getBytes()));
            assertTrue(locked.getMessage().contains("prepared transaction"));
            group.decideTransaction(id, digest, List.of(groupId), false);
            group.abortPreparedTransaction(id, digest);
            group.completeTransaction(id, digest);
            assertFalse(group.hasUnresolvedLocalTransactions());
        } finally {
            group.close(); store.close(); deleteTree(root);
        }
    }

    private static byte[] legacyPrepareCommand(String id, String digest, String group,
                                               String key, String value) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeByte(12); out.writeUTF(id); out.writeUTF(digest);
            out.writeInt(1); out.writeUTF(group); out.writeInt(1);
            out.writeUTF(key); out.writeBoolean(true); out.writeUTF(value);
        }
        return bytes.toByteArray();
    }

    private static byte[] legacySnapshot(long index, long term, String nodeId, int port,
                                         String id, String digest, String group,
                                         String key, String value) throws IOException {
        ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bodyBytes)) {
            out.writeInt(0x4b565331); out.writeLong(index); out.writeLong(term);
            out.writeBoolean(false); out.writeLong(0);
            out.writeInt(0); // ownership manifest
            out.writeInt(1); out.writeUTF(nodeId); out.writeUTF("127.0.0.1"); out.writeInt(port);
            out.writeBoolean(false); // joint voters
            out.writeBoolean(false); // pending voters
            out.writeInt(0); // values
            out.writeInt(1); // transaction records
            out.writeUTF(id); out.writeUTF(digest);
            out.writeInt(1); out.writeUTF(group);
            out.writeInt(1); out.writeUTF(key); out.writeBoolean(true); out.writeUTF(value);
            out.writeByte(-1); out.writeBoolean(true); out.writeBoolean(false); out.writeLong(term);
            out.writeInt(0); // migration intent
        }
        byte[] body = bodyBytes.toByteArray();
        CRC32 crc = new CRC32(); crc.update(body);
        ByteArrayOutputStream result = new ByteArrayOutputStream(body.length + Integer.BYTES);
        result.write(body);
        try (DataOutputStream out = new DataOutputStream(result)) { out.writeInt((int) crc.getValue()); }
        return result.toByteArray();
    }

    private static RaftGroup singleNodeGroup(String groupId, int port, Path directory, RedisStore store) throws Exception {
        return new RaftGroup(groupId, "n1", new InetSocketAddress("127.0.0.1", port), Map.of(), directory, store);
    }

    private static String keyFor(MultiRaftStore router, String groupId) {
        for (int i = 0; i < 100_000; i++) {
            String key = "large-batch-key-" + i;
            if (groupId.equals(router.groupFor(key))) return key;
        }
        throw new AssertionError("Could not find test key for " + groupId);
    }

    private static void awaitLeader(RaftGroup group) throws Exception {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            if (group.isLeader() && "n1".equals(group.getLeaderId())) return;
            Thread.sleep(10);
        }
        throw new AssertionError("Raft group did not elect its single voter: " + group.getGroupId());
    }

    private static int[] reservePorts(int count) throws Exception {
        List<ServerSocket> sockets = new ArrayList<>(); int[] ports = new int[count];
        try { for (int i = 0; i < count; i++) { sockets.add(new ServerSocket(0)); ports[i] = sockets.get(i).getLocalPort(); } }
        finally { for (ServerSocket socket : sockets) socket.close(); }
        return ports;
    }

    private static void deleteTree(Path root) throws Exception {
        if (!Files.exists(root)) return;
        try (var paths = Files.walk(root)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) Files.deleteIfExists(path);
        }
    }
}
