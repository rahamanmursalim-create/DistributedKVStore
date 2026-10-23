package com.codecrafters.redis.raft;

import com.codecrafters.redis.storage.RedisStore;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/** Injects fsync latency and verifies that Raft does not acknowledge before durable writes. */
public final class RaftSlowDiskFaultTest {
    public static void main(String[] args) throws Exception {
        Path root = Files.createTempDirectory("raft-slow-disk");
        System.setProperty("kv.test.forceDelayMillis", "35");
        try {
            Path raftDir = root.resolve("raft"), wal = root.resolve("wal.log");
            RedisStore store = new RedisStore(wal, false);
            RaftGroup group = new RaftGroup("slow", "n1", new InetSocketAddress("127.0.0.1", 0),
                    Map.of(), raftDir, store);
            try {
                group.start(); assert group.startElection();
                long started = System.nanoTime();
                group.put("durable", "ack-after-fsync".getBytes(java.nio.charset.StandardCharsets.UTF_8));
                long elapsedMillis = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
                assert elapsedMillis >= 60 : "write returned before delayed Raft log and state WAL syncs: " + elapsedMillis;
            } finally { group.close(); store.close(); }
            try (RedisStore recovered = new RedisStore(wal, false)) {
                assert "ack-after-fsync".equals(recovered.get("durable"));
            }
            System.out.println("RaftSlowDiskFaultTest passed; injected 35ms per durable force");
        } finally {
            System.clearProperty("kv.test.forceDelayMillis");
            try (var paths = Files.walk(root)) {
                paths.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
                    try { Files.deleteIfExists(path); } catch (Exception e) { throw new RuntimeException(e); }
                });
            }
        }
    }
}
