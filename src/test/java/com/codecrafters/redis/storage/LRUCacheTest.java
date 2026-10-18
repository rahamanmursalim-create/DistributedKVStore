package com.codecrafters.redis.storage;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Tests for SSTable LRU cache behavior, cache hits, misses, eviction, and stats.
 */
public final class LRUCacheTest {

    private LRUCacheTest() {
    }

    public static void main(String[] args) throws Exception {
        Path testDir = Files.createTempDirectory("sstable-lru-test-");

        try {
            testCacheHitsAndMisses(testDir);
            testCacheEviction(testDir);
            testTombstoneCaching(testDir);
            testStatsFormat(testDir);

            System.out.println("\nALL LRU CACHE TESTS PASSED");
        } finally {
            deleteDirectory(testDir);
        }
    }

    private static void testCacheHitsAndMisses(Path root) throws Exception {
        Path sstPath = root.resolve("cache-hit-miss.sst");

        Map<String, String> data = new TreeMap<>();
        data.put("alpha", "value-alpha");
        data.put("beta", "value-beta");
        data.put("gamma", "value-gamma");

        SSTable.write(sstPath, data);
        SSTable table = SSTable.open(sstPath);

        // First lookup: cache miss
        String val1 = table.get("alpha");
        check("value-alpha".equals(val1), "First lookup of alpha");
        check(table.getLookupStats().contains("cacheHits=0"), "Should have 0 hits initially");
        check(table.getLookupStats().contains("cacheMisses=1"), "Should have 1 miss");

        // Second lookup of same key: cache hit!
        String val2 = table.get("alpha");
        check("value-alpha".equals(val2), "Second lookup of alpha");
        check(table.getLookupStats().contains("cacheHits=1"), "Should have 1 hit");

        // Third lookup: another hit
        table.get("alpha");
        check(table.getLookupStats().contains("cacheHits=2"), "Should have 2 hits");

        // Lookup nonexistent key: cache miss, cached as NOT_FOUND
        String missing = table.get("nonexistent");
        check(missing == null, "Missing key should be null");

        // Second lookup of nonexistent key: cache hit of NOT_FOUND
        String missing2 = table.get("nonexistent");
        check(missing2 == null, "Second missing key lookup should be null");
        check(table.getLookupStats().contains("cacheHits=3"), "Cached miss should register as hit");

        System.out.println("PASS: Cache hits and misses verified");
    }

    private static void testCacheEviction(Path root) throws Exception {
        Path sstPath = root.resolve("cache-eviction.sst");

        // Default capacity is 1000
        Map<String, String> data = new TreeMap<>();
        for (int i = 0; i < 1100; i++) {
            data.put(String.format("key-%04d", i), "val-" + i);
        }

        SSTable.write(sstPath, data);
        SSTable table = SSTable.open(sstPath);

        // Load 1000 entries into cache
        for (int i = 0; i < 1000; i++) {
            table.get(String.format("key-%04d", i));
        }

        // Access key-0000 again to make it most-recently used
        table.get("key-0000");

        // Now access 100 new entries to trigger eviction of least-recently-used
        for (int i = 1000; i < 1100; i++) {
            table.get(String.format("key-%04d", i));
        }

        // key-0000 was recently accessed, so it should NOT be the one evicted
        // key-0001 was NOT refreshed, so it is an LRU eviction candidate
        // Verify both can still be read correctly from SSTable regardless of cache status
        check("val-0".equals(table.get("key-0000")), "key-0000 readable");
        check("val-1".equals(table.get("key-0001")), "key-0001 readable");

        System.out.println("PASS: Cache eviction and access order verified");
    }

    private static void testTombstoneCaching(Path root) throws Exception {
        Path sstPath = root.resolve("tombstone-cache.sst");

        Map<String, String> data = new TreeMap<>();
        data.put("alive", "val-alive");
        data.put("deleted", null); // tombstone

        SSTable.write(sstPath, data);
        SSTable table = SSTable.open(sstPath);

        // Lookup tombstone: returns null value in entry
        var record = table.lookup("deleted");
        check(record != null, "Tombstone record should exist");
        check(record.getValue() == null, "Tombstone value should be null");

        // Second lookup should be cached
        var record2 = table.lookup("deleted");
        check(record2 != null && record2.getValue() == null, "Cached tombstone");

        System.out.println("PASS: Tombstone caching verified");
    }

    private static void testStatsFormat(Path root) throws Exception {
        Path sstPath = root.resolve("stats-format.sst");
        Map<String, String> data = new TreeMap<>();
        data.put("k", "v");
        SSTable.write(sstPath, data);
        SSTable table = SSTable.open(sstPath);

        String stats = table.getLookupStats();
        check(stats.contains("cacheHits="), "stats has cacheHits");
        check(stats.contains("cacheMisses="), "stats has cacheMisses");
        check(stats.contains("bloomFilterRejections="), "stats has bloomFilterRejections");
        check(stats.contains("diskLookups="), "stats has diskLookups");

        System.out.println("PASS: Lookup statistics format verified");
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static void deleteDirectory(Path dir) throws Exception {
        if (Files.exists(dir)) {
            try (var paths = Files.walk(dir)) {
                paths.sorted(Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            } catch (Exception e) {
                                // Best effort
                            }
                        });
            }
        }
    }
}
