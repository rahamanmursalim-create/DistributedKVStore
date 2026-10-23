package com.codecrafters.redis.storage;

import java.nio.file.Files;
import java.nio.file.Path;

public class RedisStoreRecoveryTest {

    public static void main(String[] args) throws Exception {
        Path directory = Files.createTempDirectory("redis-store-recovery-");
        Path wal = directory.resolve("test.wal");

        try {
            // Case 1: A non-expired key survives restart.
            try (RedisStore store = new RedisStore(wal, false)) {
                store.set("persistent", "value", 10_000);
            }

            try (RedisStore store = new RedisStore(wal, false)) {
                check("Non-expired key survives restart",
                        "value".equals(store.get("persistent")));
            }

            // Case 2: An expired key must not return after restart.
            try (RedisStore store = new RedisStore(wal, false)) {
                store.set("expired", "old-value", 100);
            }

            Thread.sleep(150);

            try (RedisStore store = new RedisStore(wal, false)) {
                check("Expired key stays absent after restart",
                        store.get("expired") == null);
                boolean rejected = false;
                try { store.set("oversized", "x".repeat(1024 * 1024 + 1)); }
                catch (IllegalArgumentException expected) { rejected = true; }
                check("Per-value memory quota rejects oversized input", rejected);
            }

            System.out.println("RedisStore recovery tests completed.");
        } finally {
            Files.deleteIfExists(wal);
            Files.deleteIfExists(directory);
        }
    }

    private static void check(String name, boolean passed) {
        if (!passed) {
            throw new AssertionError("FAIL: " + name);
        }
        System.out.println("[PASS] " + name);
    }
}
