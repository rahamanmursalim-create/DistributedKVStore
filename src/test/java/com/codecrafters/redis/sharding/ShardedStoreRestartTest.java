package com.codecrafters.redis.sharding;

import com.codecrafters.redis.storage.RedisStore;

import java.nio.file.Files;
import java.nio.file.Path;

public final class ShardedStoreRestartTest {
    public static void main(String[] args) throws Exception {
        Path root = Files.createTempDirectory("sharded-restart-test");
        Path firstWal = root.resolve("first.wal"), secondWal = root.resolve("second.wal");
        try (ShardedStore initial = new ShardedStore(48)) {
            initial.addNode("first", new RedisStore(firstWal, false));
            for (int i = 0; i < 1_000; i++) initial.set("restart-" + i, "value-" + i);
            initial.addNode("second", new RedisStore(secondWal, false));
        }

        RedisStore first = new RedisStore(firstWal, false);
        RedisStore second = new RedisStore(secondWal, false);
        try (ShardedStore recovered = new ShardedStore(48)) {
            recovered.addNode("first", first);
            recovered.addNode("second", second);
            for (int i = 0; i < 1_000; i++) {
                String key = "restart-" + i;
                assert ("value-" + i).equals(recovered.get(key));
                RedisStore owner = "first".equals(recovered.ownerOf(key)) ? first : second;
                assert owner.get(key) != null;
                assert (owner == first ? second : first).get(key) == null;
            }
        }
        System.out.println("ShardedStoreRestartTest passed");
    }
}
