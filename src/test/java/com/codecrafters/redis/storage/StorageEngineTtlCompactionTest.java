
package com.codecrafters.redis.storage;

import java.nio.file.Files;
import java.nio.file.Path;

public class StorageEngineTtlCompactionTest {

    public static void main(String[] args) throws Exception {
        Path directory = Files.createTempDirectory("kv-ttl-compaction-");

        try {
            // Create multiple SSTables so compaction is actually exercised.
            try (StorageEngine engine = new StorageEngine(directory)) {
                engine.put("expiring", "old-value",
                        System.currentTimeMillis() + 300);
                engine.flush();

                engine.put("permanent", "keep-me");
                engine.flush();

                Thread.sleep(500);

                if (engine.get("expiring") != null) {
                    throw new AssertionError(
                            "Expired value should be absent before compaction");
                }

                engine.compact();

                if (engine.get("expiring") != null) {
                    throw new AssertionError(
                            "Expired value returned after compaction");
                }

                if (!"keep-me".equals(engine.get("permanent"))) {
                    throw new AssertionError(
                            "Permanent value was lost during compaction");
                }
            }

            // Verify the compacted result survives restart.
            try (StorageEngine recovered = new StorageEngine(directory)) {
                if (recovered.get("expiring") != null) {
                    throw new AssertionError(
                            "Expired value returned after restart");
                }

                if (!"keep-me".equals(recovered.get("permanent"))) {
                    throw new AssertionError(
                            "Permanent value missing after restart");
                }
            }

            System.out.println(
                    "PASSED: Expired value stays absent after compaction and restart");
            System.out.println(
                    "PASSED: Permanent value survives compaction and restart");
            System.out.println(
                    "ALL TTL COMPACTION TESTS PASSED");

        } finally {
            try (var paths = Files.walk(directory)) {
                paths.sorted(java.util.Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                Files.deleteIfExists(path);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
            }
        }
    }
}
