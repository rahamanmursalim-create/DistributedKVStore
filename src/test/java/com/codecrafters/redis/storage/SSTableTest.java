package com.codecrafters.redis.storage;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;

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


        Map<String, String> largeData = new TreeMap<>();
        for (int i = 0; i < 1000; i++) {
            largeData.put(String.format("key-%04d", i), "value-" + i);
        }

        SSTable largeTable = SSTable.write(
                dir.resolve("large.sst"), largeData);

        for (int i = 0; i < 1000; i++) {
            String key = String.format("key-%04d", i);
            if (!("value-" + i).equals(largeTable.get(key))) {
                throw new AssertionError("Lookup failed for " + key);
            }
        }

        if (largeTable.containsKey("key-1000")) {
            throw new AssertionError("Nonexistent key found");
        }

        System.out.println("PASS: 1,000-key sparse-index lookups");

        // Verify LRU cache capacity and least-recently-used eviction.
        java.lang.reflect.Field cacheField =
                SSTable.class.getDeclaredField("recordCache");
        cacheField.setAccessible(true);
        Map<String, Map.Entry<String, String>> cache =
                (Map<String, Map.Entry<String, String>>) cacheField.get(largeTable);
        if (cache.size() != 1000) {
            throw new AssertionError("Expected 1000 cached records, got " + cache.size());
        }

        Map<String, String> evictionData = new TreeMap<>();
        for (int i = 0; i <= 1000; i++) {
            evictionData.put(String.format("key-%04d", i), "value-" + i);
        }
        SSTable evictionTable = SSTable.write(dir.resolve("eviction.sst"), evictionData);
        for (int i = 0; i <= 1000; i++) {
            evictionTable.get(String.format("key-%04d", i));
        }
        Map<String, Map.Entry<String, String>> evictionCache =
                (Map<String, Map.Entry<String, String>>) cacheField.get(evictionTable);
        if (evictionCache.size() != 1000) {
            throw new AssertionError("LRU cache exceeded capacity: " + evictionCache.size());
        }
        if (evictionCache.containsKey("key-0000")) {
            throw new AssertionError("Least recently used entry was not evicted");
        }
        System.out.println("PASS: LRU cache capacity and eviction");

        System.out.println("SSTable baseline tests passed");
    }

    @Test
    void runsExistingScenarioUnderJUnit() throws Exception {
        main(new String[0]);
    }
}
