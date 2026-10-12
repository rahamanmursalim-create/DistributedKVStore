package com.codecrafters.redis.storage;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Thread-safe sorted in-memory table.
 * A null value represents a deletion tombstone.
 */
public final class MemTable {

    private final NavigableMap<String, String> entries = new TreeMap<>();
    private long estimatedSizeBytes = 0;

    public synchronized void put(String key, String value) {
        if (key == null || value == null) {
            throw new IllegalArgumentException(
                    "Key and value cannot be null");
        }

        String previous = entries.put(key, value);

        if (previous != null) {
            estimatedSizeBytes -= estimateEntrySize(key, previous);
        }

        estimatedSizeBytes += estimateEntrySize(key, value);
    }

    public synchronized String get(String key) {
        return key == null ? null : entries.get(key);
    }

    public synchronized boolean containsKey(String key) {
        return key != null && entries.containsKey(key);
    }

    public synchronized String delete(String key) {
        if (key == null) {
            return null;
        }

        String previous = entries.put(key, null);

        if (previous != null) {
            estimatedSizeBytes -= estimateEntrySize(key, previous);
        }

        estimatedSizeBytes += estimateEntrySize(key, "");
        return previous;
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

    public synchronized Map<String, String> snapshot() {
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