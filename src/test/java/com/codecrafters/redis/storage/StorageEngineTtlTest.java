package com.codecrafters.redis.storage;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

public class StorageEngineTtlTest {

    public static void main(String[] args) throws Exception {
        Path directory = Files.createTempDirectory("storage-engine-ttl-test");
        StorageEngine engine = null;

        try {
            long expiry = System.currentTimeMillis() + 5000;

            engine = new StorageEngine(directory);
            engine.put("temporary", "alive", expiry);
            engine.put("permanent", "saved");

            check("alive".equals(engine.get("temporary")),
                    "TTL value readable before expiry");

            engine.flush();

            check("alive".equals(engine.get("temporary")),
                    "TTL value readable after flush");

            engine.close();
            engine = null;

            engine = new StorageEngine(directory);

            check("alive".equals(engine.get("temporary")),
                    "TTL value survives restart before expiry");

            check("saved".equals(engine.get("permanent")),
                    "Permanent value survives restart");

            engine.close();
            engine = null;

            long wait = expiry - System.currentTimeMillis() + 100;
            if (wait > 0) {
                Thread.sleep(wait);
            }

            engine = new StorageEngine(directory);

            check(engine.get("temporary") == null,
                    "Expired value stays absent after restart");

            check("saved".equals(engine.get("permanent")),
                    "Permanent value remains available");

            System.out.println("ALL STORAGE ENGINE TTL TESTS PASSED");
        } finally {
            if (engine != null) {
                engine.close();
            }

            try (var paths = Files.walk(directory)) {
                paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }
    }

    private static void check(boolean condition, String description) {
        if (!condition) {
            throw new AssertionError("FAILED: " + description);
        }
        System.out.println("PASSED: " + description);
    }
}
