package com.codecrafters.redis.storage;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;

public final class RedisStoreBPlusTreeTest {
    public static void main(String[] args) throws Exception {
        Path directory = Files.createTempDirectory("redis-btree-index");
        Path wal = directory.resolve("wal.log");
        try {
            try (RedisStore store = new RedisStore(wal, false)) {
                LinkedHashMap<String, String> batch = new LinkedHashMap<>();
                for (int i = 0; i < 4_000; i++) batch.put(String.format("key-%05d", i), "value-" + i);
                store.setBatch(batch);
                assert store.size() == 4_000;
                for (int i = 0; i < 4_000; i += 17) {
                    assert store.get(String.format("key-%05d", i)).equals("value-" + i);
                }

                String[] deletes = new String[1_000];
                for (int i = 0; i < deletes.length; i++) deletes[i] = String.format("key-%05d", i * 2);
                assert store.del(deletes) == deletes.length;
                assert store.size() == 3_000;
                assert store.get("key-00000") == null;
                assert store.get("key-00001").equals("value-1");

                store.set("expiring", "soon", 20);
                Thread.sleep(40);
                assert store.size() == 3_000;
                assert store.get("expiring") == null;
            }

            try (RedisStore recovered = new RedisStore(wal, false)) {
                assert recovered.size() == 3_000;
                assert recovered.get("key-00000") == null;
                assert recovered.get("key-00001").equals("value-1");
                assert recovered.get("expiring") == null;
            }
            System.out.println("RedisStoreBPlusTreeTest passed");
        } finally {
            try (var paths = Files.walk(directory)) {
                paths.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
                    try { Files.deleteIfExists(path); }
                    catch (Exception e) { throw new RuntimeException(e); }
                });
            }
        }
    }
}
