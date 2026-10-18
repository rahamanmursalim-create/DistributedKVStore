package com.codecrafters.redis.storage;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * Unit tests for the MemTable class.
 */
public final class MemTableTest {

    private MemTableTest() {
    }

    public static void main(String[] args) throws Exception {
        testPutAndGet();
        testNullKeyValueRejected();
        testOverwrite();
        testDeleteReturnsPrevious();
        testDeleteNullKey();
        testContainsKey();
        testSizeCounts();
        testIsEmpty();
        testEstimatedSizeBytes();
        testSnapshot();
        testSnapshotRecords();
        testClear();
        testTtlExpiry();
        testThreadSafety();

        System.out.println("ALL MEMTABLE TESTS PASSED");
    }

    private static void testPutAndGet() {
        MemTable table = new MemTable();
        table.put("name", "Mursalim");
        check("Mursalim".equals(table.get("name")),
                "Put and get should return stored value");
        System.out.println("TEST 1 PASSED: Put and get");
    }

    private static void testNullKeyValueRejected() {
        MemTable table = new MemTable();

        boolean nullKeyRejected = false;
        try {
            table.put(null, "value");
        } catch (IllegalArgumentException e) {
            nullKeyRejected = true;
        }
        check(nullKeyRejected, "Null key should be rejected");

        boolean nullValueRejected = false;
        try {
            table.put("key", null);
        } catch (IllegalArgumentException e) {
            nullValueRejected = true;
        }
        check(nullValueRejected, "Null value should be rejected");

        System.out.println(
                "TEST 2 PASSED: Null key/value rejected");
    }

    private static void testOverwrite() {
        MemTable table = new MemTable();
        table.put("key", "first");
        table.put("key", "second");
        check("second".equals(table.get("key")),
                "Overwrite should return latest value");
        System.out.println("TEST 3 PASSED: Overwrite returns latest");
    }

    private static void testDeleteReturnsPrevious() {
        MemTable table = new MemTable();
        table.put("key", "value");
        String previous = table.delete("key");
        check("value".equals(previous),
                "Delete should return previous value");
        check(table.get("key") == null,
                "Deleted key should return null");
        System.out.println("TEST 4 PASSED: Delete returns previous value");
    }

    private static void testDeleteNullKey() {
        MemTable table = new MemTable();
        String result = table.delete(null);
        check(result == null, "Delete null key should return null");
        System.out.println("TEST 5 PASSED: Delete null key returns null");
    }

    private static void testContainsKey() {
        MemTable table = new MemTable();
        check(!table.containsKey("key"),
                "Missing key should not be contained");
        table.put("key", "value");
        check(table.containsKey("key"),
                "Existing key should be contained");
        table.delete("key");
        check(table.containsKey("key"),
                "Tombstoned key still has an entry");
        check(!table.containsKey(null),
                "Null key should not be contained");
        System.out.println("TEST 6 PASSED: containsKey behavior");
    }

    private static void testSizeCounts() {
        MemTable table = new MemTable();
        check(table.size() == 0, "Empty table size should be 0");
        table.put("a", "1");
        table.put("b", "2");
        check(table.size() == 2, "Size should be 2 after two puts");
        table.delete("a");
        check(table.size() == 2,
                "Size should still be 2 after delete (tombstone)");
        System.out.println("TEST 7 PASSED: Size counts entries");
    }

    private static void testIsEmpty() {
        MemTable table = new MemTable();
        check(table.isEmpty(), "New table should be empty");
        table.put("key", "value");
        check(!table.isEmpty(), "Table with entry should not be empty");
        System.out.println("TEST 8 PASSED: isEmpty behavior");
    }

    private static void testEstimatedSizeBytes() {
        MemTable table = new MemTable();
        check(table.estimatedSizeBytes() == 0,
                "Empty table should have 0 estimated bytes");
        table.put("key", "value");
        long afterPut = table.estimatedSizeBytes();
        check(afterPut > 0,
                "Estimated bytes should increase after put");
        table.put("key", "longer-value");
        long afterOverwrite = table.estimatedSizeBytes();
        check(afterOverwrite != afterPut,
                "Estimated bytes should change on overwrite");
        System.out.println("TEST 9 PASSED: Estimated size bytes");
    }

    private static void testSnapshot() {
        MemTable table = new MemTable();
        table.put("a", "1");
        table.put("b", "2");
        table.delete("b");
        var snap = table.snapshot();
        check("1".equals(snap.get("a")),
                "Snapshot should contain live value");
        check(snap.get("b") == null,
                "Snapshot should have null for tombstone");
        check(snap.containsKey("b"),
                "Snapshot should contain tombstone key");
        System.out.println("TEST 10 PASSED: Snapshot behavior");
    }

    private static void testSnapshotRecords() {
        MemTable table = new MemTable();
        table.put("key", "value", 999999999999L);
        var records = table.snapshotRecords();
        StorageRecord record = records.get("key");
        check(record != null, "Record should exist");
        check("value".equals(record.getValue()),
                "Record value should match");
        check(record.getExpiryEpochMillis() == 999999999999L,
                "Record expiry should match");
        System.out.println("TEST 11 PASSED: Snapshot records");
    }

    private static void testClear() {
        MemTable table = new MemTable();
        table.put("a", "1");
        table.put("b", "2");
        table.clear();
        check(table.isEmpty(), "Table should be empty after clear");
        check(table.size() == 0, "Size should be 0 after clear");
        check(table.estimatedSizeBytes() == 0,
                "Estimated bytes should be 0 after clear");
        System.out.println("TEST 12 PASSED: Clear empties everything");
    }

    private static void testTtlExpiry() throws InterruptedException {
        MemTable table = new MemTable();
        long expiry = System.currentTimeMillis() + 200;
        table.put("temp", "alive", expiry);
        check("alive".equals(table.get("temp")),
                "TTL key should be readable before expiry");

        Thread.sleep(300);

        check(table.get("temp") == null,
                "TTL key should be null after expiry");

        // The record still exists physically.
        check(table.containsKey("temp"),
                "TTL key entry should still exist in table");
        StorageRecord record = table.getRecord("temp");
        check(record != null && record.isExpired(),
                "Record should be marked as expired");
        System.out.println("TEST 13 PASSED: TTL expiry");
    }

    private static void testThreadSafety() throws Exception {
        MemTable table = new MemTable();
        int threadCount = 8;
        int opsPerThread = 200;
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            futures.add(pool.submit(() -> {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    return;
                }
                for (int i = 0; i < opsPerThread; i++) {
                    String key = "t" + threadId + "-k" + i;
                    table.put(key, "v" + i);
                    table.get(key);
                    if (i % 3 == 0) {
                        table.delete(key);
                    }
                }
            }));
        }

        latch.countDown();
        for (Future<?> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }
        pool.shutdown();

        // Verify no crash and the table is in a consistent state.
        check(table.size() > 0, "Table should have entries after concurrent ops");
        table.snapshot(); // Should not throw.
        System.out.println("TEST 14 PASSED: Thread safety");
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
