package com.codecrafters.redis.storage;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Thread-safe sorted in-memory table.
 * A null record represents a deletion tombstone.
 */
public final class MemTable {

    private final NavigableMap<String, StorageRecord> entries = new TreeMap<>();
    private long estimatedSizeBytes = 0;

    public synchronized void put(String key, String value) {
        put(key, value, -1L);
    }

    public synchronized void put(
            String key, String value, long expiryEpochMillis) {
        if (key == null || value == null) {
            throw new IllegalArgumentException("Key and value cannot be null");
        }

        StorageRecord record = new StorageRecord(value, expiryEpochMillis);
        replaceEntry(key, record, value);
    }

    public synchronized String get(String key) {
        StorageRecord record = getRecord(key);
        return record == null || record.isExpired() ? null : record.getValue();
    }

    public synchronized StorageRecord getRecord(String key) {
        return key == null ? null : entries.get(key);
    }

    public synchronized boolean containsKey(String key) {
        return key != null && entries.containsKey(key);
    }

    public synchronized String delete(String key) {
        if (key == null) {
            return null;
        }

        StorageRecord previous = entries.get(key);
        String previousValue = previous == null ? null : previous.getValue();

        replaceEntry(key, null, "");
        return previousValue;
    }

    private void replaceEntry(String key, StorageRecord record, String sizeValue) {
        boolean existed = entries.containsKey(key);
        StorageRecord previous = entries.get(key);

        if (existed) {
            estimatedSizeBytes -= estimateEntrySize(
                    key, previous == null ? "" : previous.getValue());
        }

        entries.put(key, record);
        estimatedSizeBytes += estimateEntrySize(key, sizeValue);
    }

    public synchronized int size() {
        return entries.size();
    }

    public synchronized boolean isEmpty() {
        return entries.isEmpty();
    }

    public synchronized long estimatedSizeBytes() {
        return estimatedSizeBytes;
    }

    /**
     * Compatibility snapshot: values and null tombstones, without expiry metadata.
     */
    public synchronized Map<String, String> snapshot() {
        Map<String, String> result = new TreeMap<>();
        entries.forEach((key, record) ->
                result.put(key, record == null ? null : record.getValue()));
        return Collections.unmodifiableMap(result);
    }

    /**
     * Snapshot including expiry metadata and tombstones.
     */
    public synchronized Map<String, StorageRecord> snapshotRecords() {
        return Collections.unmodifiableMap(new TreeMap<>(entries));
    }

    public synchronized void clear() {
        entries.clear();
        estimatedSizeBytes = 0;
    }

    private long estimateEntrySize(String key, String value) {
        return 32L
                + key.getBytes(StandardCharsets.UTF_8).length
                + value.getBytes(StandardCharsets.UTF_8).length;
    }
}