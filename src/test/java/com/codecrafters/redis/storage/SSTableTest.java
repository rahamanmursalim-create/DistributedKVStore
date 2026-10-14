package com.codecrafters.redis.storage;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class SSTableTest {
    public static void main(String[] args) throws Exception {
        Path dir = Path.of("test-data", "sstable-lookup");
        Files.createDirectories(dir);
        Path file = dir.resolve("test.sst");
        Files.deleteIfExists(file);

        SSTable table = SSTable.write(file, Map.of(
                "alpha", "one",
                "beta", "two",
                "gamma", "three"
        ));

        if (!"two".equals(table.get("beta"))) {
            throw new AssertionError("Existing key lookup failed");
        }
        if (!table.containsKey("alpha")) {
            throw new AssertionError("containsKey failed");
        }
        if (table.get("missing") != null) {
            throw new AssertionError("Missing key should return null");
        }

        System.out.println("SSTable baseline tests passed");
    }
}
