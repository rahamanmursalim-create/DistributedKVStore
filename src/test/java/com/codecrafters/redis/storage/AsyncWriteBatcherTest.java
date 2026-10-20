package com.codecrafters.redis.storage;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public final class AsyncWriteBatcherTest {
    public static void main(String[] args) throws Exception {
        Path directory = Files.createTempDirectory("redis-batched-writes-");
        Path wal = directory.resolve("store.wal");
        RedisStore store = new RedisStore(wal, false);
        AsyncWriteBatcher batcher = new AsyncWriteBatcher(store, 1024, 64, 5);
        List<CompletableFuture<Void>> writes = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            writes.add(batcher.submit("key-" + i, "value-" + i));
        }
        CompletableFuture.allOf(writes.toArray(CompletableFuture[]::new)).get(30, java.util.concurrent.TimeUnit.SECONDS);
        batcher.close();
        store.close();

        try (RedisStore recovered = new RedisStore(wal, false)) {
            for (int i = 0; i < 500; i++) {
                if (!("value-" + i).equals(recovered.get("key-" + i)))
                    throw new AssertionError("Missing durable batched write key-" + i);
            }
        }

        try {
            batcher.submit("late", "write");
            throw new AssertionError("Closed batcher accepted a write");
        } catch (IllegalStateException expected) {
            System.out.println("PASS: closed batcher rejects new writes");
        }
        System.out.println("PASS: all 500 batched writes recovered after restart");
    }
}
