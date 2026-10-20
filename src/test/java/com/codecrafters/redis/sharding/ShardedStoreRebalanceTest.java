package com.codecrafters.redis.sharding;

import com.codecrafters.redis.storage.RedisStore;
import java.nio.file.Files;
import java.nio.file.Path;

public final class ShardedStoreRebalanceTest {
    public static void main(String[] args) throws Exception {
        Path root = Files.createTempDirectory("sharded-store-test");
        RedisStore first = new RedisStore(root.resolve("one.wal"), false);
        RedisStore second = new RedisStore(root.resolve("two.wal"), false);
        RedisStore third = new RedisStore(root.resolve("three.wal"), false);
        ShardedStore sharded = new ShardedStore(64);
        try {
            sharded.addNode("one", first);
            for (int i = 0; i < 2_000; i++) sharded.set("key-" + i, "value-" + i);
            sharded.addNode("two", second);
            assert sharded.nodeIds().size() == 2;
            int moved = 0;
            for (int i = 0; i < 2_000; i++) {
                String key = "key-" + i;
                assert ("value-" + i).equals(sharded.get(key));
                if ("two".equals(sharded.ownerOf(key))) {
                    moved++;
                    assert first.get(key) == null;
                    assert ("value-" + i).equals(second.get(key));
                } else assert ("value-" + i).equals(first.get(key));
            }
            assert moved > 0 && moved < 2_000;
            sharded.set("updated", "before-third"); sharded.addNode("three", third);
            assert "before-third".equals(sharded.get("updated"));
            String ttlKey = null;
            for (int i = 0; i < 10_000; i++) {
                String candidate = "ttl-" + i;
                if (!sharded.ownerOf(candidate).equals("one")) continue;
                ttlKey = candidate; break;
            }
            assert ttlKey != null;
            first.set(ttlKey, "expires", 30_000);
            long expiry = first.getEntry(ttlKey).getExpiryTimestampMillis();
            RedisStore fourth = new RedisStore(root.resolve("four.wal"), false);
            sharded.addNode("four", fourth);
            assert "expires".equals(sharded.get(ttlKey));
            RedisStore ttlOwner = switch (sharded.ownerOf(ttlKey)) {
                case "one" -> first;
                case "two" -> second;
                case "three" -> third;
                case "four" -> fourth;
                default -> throw new AssertionError("Unknown owner");
            };
            assert ttlOwner.getEntry(ttlKey).getExpiryTimestampMillis() == expiry;
            for (int i = 0; i < 2_000; i++) assert ("value-" + i).equals(sharded.get("key-" + i));
            sharded.removeNode("two"); assert !sharded.nodeIds().contains("two");
            for (int i = 0; i < 2_000; i++) {
                String key = "key-" + i; assert ("value-" + i).equals(sharded.get(key)); assert second.get(key) == null;
            }
            assert sharded.delete("key-20"); assert sharded.get("key-20") == null;
            System.out.println("ShardedStoreRebalanceTest passed; moved=" + moved);
        } finally { sharded.close(); second.close(); }
    }
}
