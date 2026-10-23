package com.codecrafters.redis.storage;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public final class RedisStoreRocksDBIntegrationTest {
    public static void main(String[] args) throws Exception {
        Path directory = Files.createTempDirectory("redis-rocksdb-backend");
        try (RedisStore store = RedisStore.usingRocksDB(directory, false)) {
            store.set("plain", "durable");
            store.set("expiring", "short-lived", 30_000);
            store.set("expired", "gone-soon", 100);
            store.setBatch(Map.of("batch-a", "A", "batch-b", "B"));
            try (MVCCTransaction tx = store.beginTransaction()) {
                assert "durable".equals(tx.get("plain"));
                tx.put("transaction", "committed");
                tx.delete("batch-a");
                tx.commit();
            }
            Thread.sleep(150);
            assert store.get("expired") == null;
        }
        try (RedisStore recovered = RedisStore.usingRocksDB(directory, false)) {
            assert "durable".equals(recovered.get("plain"));
            assert "short-lived".equals(recovered.get("expiring"));
            assert "B".equals(recovered.get("batch-b"));
            assert recovered.get("batch-a") == null;
            assert "committed".equals(recovered.get("transaction"));
            assert recovered.size() == 4;
            recovered.del("plain");
        }
        try (RedisStore recovered = RedisStore.usingRocksDB(directory, false)) {
            assert recovered.get("plain") == null;
            assert recovered.get("transaction") != null;
            recovered.replaceSnapshot(Map.of("snapshot-row", new ValueEntry("snapshot-value", System.currentTimeMillis() + 30_000)));
            assert recovered.get("transaction") == null;
            assert "snapshot-value".equals(recovered.get("snapshot-row"));
        }
        try (RedisStore recovered = RedisStore.usingRocksDB(directory, false)) {
            assert recovered.get("transaction") == null;
            assert "snapshot-value".equals(recovered.get("snapshot-row"));
        }
        System.out.println("RedisStoreRocksDBIntegrationTest passed");
    }
}
