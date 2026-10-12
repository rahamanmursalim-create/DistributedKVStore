
package com.codecrafters.redis.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * LSM-style storage engine using a MemTable and immutable SSTables.
 *
 * Newer SSTables take precedence over older SSTables.
 * Null values in storage represent deletion tombstones.
 */
public final class StorageEngine {

    private static final Pattern SSTABLE_NAME =
            Pattern.compile("sstable-(\\d+)\\.sst");

    private final MemTable memTable = new MemTable();
    private final Path directory;
    private final List<SSTable> ssTables = new ArrayList<>();

    private long nextTableId = 1;

    public StorageEngine(Path directory) throws IOException {
        this.directory = directory.toAbsolutePath().normalize();

        Files.createDirectories(this.directory);
        loadExistingSSTables();
    }

    /**
     * Loads existing SSTables in ascending ID order.
     */
    private void loadExistingSSTables() throws IOException {
        List<Path> files;

        try (Stream<Path> paths = Files.list(directory)) {
            files = paths
                    .filter(Files::isRegularFile)
                    .filter(path -> SSTABLE_NAME.matcher(
                            path.getFileName().toString()).matches())
                    .sorted(Comparator.comparingLong(
                            StorageEngine::extractTableId))
                    .toList();
        }

        for (Path file : files) {
            long id = extractTableId(file);

            SSTable table = SSTable.open(file);

            // Validate the SSTable before registering it.
            table.readAll();

            ssTables.add(table);
            nextTableId = Math.max(nextTableId, id + 1);
        }
    }

    private static long extractTableId(Path file) {
        Matcher matcher = SSTABLE_NAME.matcher(
                file.getFileName().toString());

        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                    "Invalid SSTable filename: " + file);
        }

        return Long.parseLong(matcher.group(1));
    }

    /**
     * Inserts or updates a key in the MemTable.
     */
    public synchronized void put(String key, String value) {
        if (key == null || value == null) {
            throw new IllegalArgumentException(
                    "Key and value cannot be null");
        }

        memTable.put(key, value);
    }

    /**
     * Retrieves a value.
     * Returns null if the key is missing or deleted.
     */
    public synchronized String get(String key) throws IOException {
        if (key == null) {
            return null;
        }

        // The MemTable always contains the latest in-memory changes.
        if (memTable.containsKey(key)) {
            return memTable.get(key);
        }

        // Search newest SSTables before older SSTables.
        for (int i = ssTables.size() - 1; i >= 0; i--) {
            SSTable table = ssTables.get(i);

            if (table.containsKey(key)) {
                // A null value is a tombstone.
                // Stop searching so older values cannot reappear.
                return table.get(key);
            }
        }

        return null;
    }

    /**
     * Deletes a key by inserting a tombstone.
     *
     * Returns true if a non-null value existed before deletion.
     * A tombstone is recorded even if the key is already absent.
     */
    public synchronized boolean delete(String key) throws IOException {
        if (key == null) {
            throw new IllegalArgumentException(
                    "Key cannot be null");
        }

        String previousValue = get(key);

        // Store the tombstone to prevent resurrection from older tables.
        memTable.delete(key);

        return previousValue != null;
    }

    /**
     * Flushes the MemTable into a new immutable SSTable.
     */
    public synchronized void flush() throws IOException {
        if (memTable.isEmpty()) {
            return;
        }

        Path tablePath = directory.resolve(
                String.format("sstable-%020d.sst", nextTableId));

        Map<String, String> entries = memTable.snapshot();

        SSTable written = SSTable.write(tablePath, entries);

        ssTables.add(written);
        memTable.clear();
        nextTableId++;
    }

    public synchronized int memTableSize() {
        return memTable.size();
    }

    public synchronized int sstableCount() {
        return ssTables.size();
    }

    /**
     * Returns the visible key-value state.
     * Tombstones remove keys from the resulting snapshot.
     */
    public synchronized NavigableMap<String, String> snapshot()
            throws IOException {

        NavigableMap<String, String> result = new TreeMap<>();

        // Apply older tables first, then newer tables.
        for (SSTable table : ssTables) {
            applyEntries(result, table.readAll());
        }

        // In-memory entries override all disk entries.
        applyEntries(result, memTable.snapshot());

        return result;
    }

    /**
     * Applies entries while interpreting null values as tombstones.
     */
    private static void applyEntries(
            NavigableMap<String, String> target,
            Map<String, String> entries) {

        for (Map.Entry<String, String> entry : entries.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            if (value == null) {
                target.remove(key);
            } else {
                target.put(key, value);
            }
        }
    }
}
