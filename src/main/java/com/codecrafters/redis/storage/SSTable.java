package com.codecrafters.redis.storage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.AbstractMap;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Immutable, sorted on-disk table for the LSM storage engine.
 *
 * Supported record formats:
 * encodedKey<TAB>D:                    -> deletion tombstone
 * encodedKey<TAB>V:encodedValue        -> normal value
 * encodedKey<TAB>T:expiry:encodedValue -> value with expiry metadata
 * encodedKey<TAB>encodedValue          -> legacy value
 *
 * Expiry -1 means the value never expires.
 */
public final class SSTable {

    private final Path file;

    private static final int SPARSE_INDEX_INTERVAL = 128;
    private static final int DEFAULT_LRU_CACHE_CAPACITY = 1000;

    private static final int LRU_CACHE_CAPACITY = Math.max(
            1,
            Integer.getInteger(
                    "sstable.cache.capacity",
                    DEFAULT_LRU_CACHE_CAPACITY
            )
    );

    // Cached sentinel distinguishes a cached miss from an uncached lookup.
    private static final Map.Entry<String, String> NOT_FOUND =
            new AbstractMap.SimpleImmutableEntry<>(null, null);

    private volatile NavigableMap<String, Long> keyOffsets;
    private volatile BloomFilter bloomFilter;

    private final Map<String, Map.Entry<String, String>> recordCache =
            new LinkedHashMap<>(128, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(
                        Map.Entry<String, Map.Entry<String, String>> eldest) {
                    return size() > LRU_CACHE_CAPACITY;
                }
            };

    // Diagnostic counters. They do not affect lookup results.
    private final LongAdder cacheHits = new LongAdder();
    private final LongAdder cacheMisses = new LongAdder();
    private final LongAdder bloomFilterRejections = new LongAdder();
    private final LongAdder diskLookups = new LongAdder();

    private SSTable(Path file) {
        this.file = file.toAbsolutePath().normalize();
    }

    /**
     * Writes entries using the V:/D: format.
     * A null value represents a deletion tombstone.
     */
    public static SSTable write(
            Path file,
            Map<String, String> entries
    ) throws IOException {

        Path absolute = file.toAbsolutePath().normalize();
        Path parent = absolute.getParent();

        if (parent != null) {
            Files.createDirectories(parent);
        }

        Path temp = Files.createTempFile(
                parent, absolute.getFileName().toString(), ".tmp");

        boolean published = false;

        try {
            NavigableMap<String, String> sorted = new TreeMap<>(entries);

            try (var channel = java.nio.channels.FileChannel.open(
                    temp,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING);
                 var output = new java.io.BufferedOutputStream(
                    java.nio.channels.Channels.newOutputStream(channel));
                 var writer = new java.io.BufferedWriter(
                    new java.io.OutputStreamWriter(
                    output, StandardCharsets.UTF_8))) {

                for (Map.Entry<String, String> entry : sorted.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();

                    if (key == null) {
                        throw new IOException("SSTable key cannot be null");
                    }

                    writer.write(encode(key));
                    writer.write('\t');

                    if (value == null) {
                        writer.write("D:");
                    } else {
                        writer.write("V:");
                        writer.write(encode(value));
                    }

                    writer.newLine();
                }

                writer.flush();
                channel.force(true);
            }

            try {
                Files.move(
                        temp,
                        absolute,
                        java.nio.file.StandardCopyOption.ATOMIC_MOVE);
            } catch (java.nio.file.AtomicMoveNotSupportedException e) {
                Files.move(temp, absolute);
            }

            published = true;
            return new SSTable(absolute);

        } finally {
            if (!published) {
                Files.deleteIfExists(temp);
            }
        }
    }

    /**
     * Writes records with expiry metadata.
     * Null records represent deletion tombstones.
     */
    public static SSTable writeRecords(
            Path file,
            Map<String, StorageRecord> records
    ) throws IOException {

        Path absolute = file.toAbsolutePath().normalize();
        Path parent = absolute.getParent();

        if (parent != null) {
            Files.createDirectories(parent);
        }

        Path temp = Files.createTempFile(
                parent, absolute.getFileName().toString(), ".tmp");

        boolean published = false;

        try {
            NavigableMap<String, StorageRecord> sorted =
                    new TreeMap<>(records);

            try (var channel = java.nio.channels.FileChannel.open(
                    temp,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING);
                 var output = new java.io.BufferedOutputStream(
                    java.nio.channels.Channels.newOutputStream(channel));
                 var writer = new java.io.BufferedWriter(
                    new java.io.OutputStreamWriter(
                    output, StandardCharsets.UTF_8))) {

                for (Map.Entry<String, StorageRecord> entry
                        : sorted.entrySet()) {

                    String key = entry.getKey();

                    if (key == null) {
                        throw new IOException("SSTable key cannot be null");
                    }

                    writer.write(encode(key));
                    writer.write('\t');

                    StorageRecord record = entry.getValue();

                    if (record == null) {
                        writer.write("D:");
                    } else {
                        writer.write("T:");
                        writer.write(Long.toString(
                                record.getExpiryEpochMillis()));
                        writer.write(':');
                        writer.write(encode(record.getValue()));
                    }

                    writer.newLine();
                }

                writer.flush();
                channel.force(true);
            }

            try {
                Files.move(
                        temp,
                        absolute,
                        java.nio.file.StandardCopyOption.ATOMIC_MOVE);
            } catch (java.nio.file.AtomicMoveNotSupportedException e) {
                Files.move(temp, absolute);
            }

            published = true;
            return new SSTable(absolute);

        } finally {
            if (!published) {
                Files.deleteIfExists(temp);
            }
        }
    }

    /**
     * Opens an existing SSTable.
     */
    public static SSTable open(Path file) throws IOException {
        Path absolute = file.toAbsolutePath().normalize();

        if (!Files.isRegularFile(absolute)) {
            throw new IOException("SSTable file not found: " + absolute);
        }

        return new SSTable(absolute);
    }

    /**
     * Returns the value for a key.
     * Returns null when absent, deleted, or expired.
     */
    public String get(String key) throws IOException {
        Map.Entry<String, String> record = findRecord(key);
        return record == null ? null : record.getValue();
    }

    /**
     * Returns true if a record exists, including a tombstone.
     * An expired record still exists physically in the SSTable.
     */
    public boolean containsKey(String key) throws IOException {
        return findRecord(key) != null;
    }

    /**
     * Returns the stored record, or null if the key is absent.
     * A record with a null value represents a tombstone or expired value.
     */
    public Map.Entry<String, String> lookup(String key) throws IOException {
        return findRecord(key);
    }

    /**
     * Returns diagnostic statistics for this SSTable.
     */
    public String getLookupStats() {
        return "cacheHits=" + cacheHits.sum()
                + ", cacheMisses=" + cacheMisses.sum()
                + ", bloomFilterRejections=" + bloomFilterRejections.sum()
                + ", diskLookups=" + diskLookups.sum();
    }

    private Map.Entry<String, String> findRecord(String key)
            throws IOException {

        if (key == null) {
            return null;
        }

        // The access-ordered LRU cache also stores negative lookups.
        synchronized (recordCache) {
            Map.Entry<String, String> cached = recordCache.get(key);

            if (cached != null) {
                cacheHits.increment();
                return cached == NOT_FOUND ? null : cached;
            }
        }

        cacheMisses.increment();

        ensureIndex();

        // A Bloom-filter miss is definitive for this immutable SSTable.
        if (!bloomFilter.mightContain(key)) {
            bloomFilterRejections.increment();

            synchronized (recordCache) {
                recordCache.put(key, NOT_FOUND);
            }

            return null;
        }

        Map.Entry<String, Long> floor = keyOffsets.floorEntry(key);
        long offset = floor == null ? 0L : floor.getValue();

        diskLookups.increment();

        try (RandomAccessFile raf =
                     new RandomAccessFile(file.toFile(), "r")) {

            raf.seek(offset);
            String line;

            while ((line = raf.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }

                Map.Entry<String, String> record = parseRecord(line);
                int comparison = record.getKey().compareTo(key);

                if (comparison == 0) {
                    // TTL records are deliberately not cached because their
                    // value may expire while this SSTable remains open.
                    if (!line.contains("\tT:")) {
                        synchronized (recordCache) {
                            recordCache.put(key, record);
                        }
                    }

                    return record;
                }

                if (comparison > 0) {
                    synchronized (recordCache) {
                        recordCache.put(key, NOT_FOUND);
                    }

                    return null;
                }
            }
        }

        synchronized (recordCache) {
            recordCache.put(key, NOT_FOUND);
        }

        return null;
    }

    /**
     * Builds the sparse index and Bloom filter.
     */
    private void ensureIndex() throws IOException {
        if (keyOffsets != null) {
            return;
        }

        synchronized (this) {
            if (keyOffsets != null) {
                return;
            }

            if (!Files.isRegularFile(file)) {
                throw new IOException("SSTable file not found: " + file);
            }

            NavigableMap<String, Long> sparseIndex = new TreeMap<>();
            int recordCount = 0;

            try (RandomAccessFile raf =
                         new RandomAccessFile(file.toFile(), "r")) {

                String line;

                while (true) {
                    long offset = raf.getFilePointer();
                    line = raf.readLine();

                    if (line == null) {
                        break;
                    }

                    if (line.isEmpty()) {
                        continue;
                    }

                    Map.Entry<String, String> record = parseRecord(line);

                    if (recordCount % SPARSE_INDEX_INTERVAL == 0) {
                        sparseIndex.put(record.getKey(), offset);
                    }

                    recordCount++;
                }
            }

            BloomFilter filter = new BloomFilter(
                    Math.max(1, recordCount));

            try (RandomAccessFile raf =
                         new RandomAccessFile(file.toFile(), "r")) {

                String line;

                while ((line = raf.readLine()) != null) {
                    if (!line.isEmpty()) {
                        filter.add(parseRecord(line).getKey());
                    }
                }
            }

            bloomFilter = filter;
            keyOffsets = sparseIndex;
        }
    }

    /**
     * Parses every supported SSTable record format.
     */
    private Map.Entry<String, String> parseRecord(String line)
            throws IOException {

        if (line == null) {
            throw new IOException("Unexpected end of SSTable: " + file);
        }

        String[] parts = line.split("\\t", -1);

        if (parts.length != 2) {
            throw new IOException("Invalid SSTable record in " + file);
        }

        try {
            String key = decode(parts[0]);
            String encodedValue = parts[1];
            String value;

            if (encodedValue.startsWith("D:")) {
                value = null;

            } else if (encodedValue.startsWith("T:")) {
                int separator = encodedValue.indexOf(':', 2);

                if (separator < 0) {
                    throw new IOException("Invalid TTL record in " + file);
                }

                long expiry = Long.parseLong(
                        encodedValue.substring(2, separator));

                value = decode(encodedValue.substring(separator + 1));

                if (expiry >= 0
                        && System.currentTimeMillis() >= expiry) {
                    value = null;
                }

            } else if (encodedValue.startsWith("V:")) {
                value = decode(encodedValue.substring(2));

            } else {
                // Backward compatibility with legacy unmarked Base64 values.
                value = decode(encodedValue);
            }

            return new AbstractMap.SimpleImmutableEntry<>(key, value);

        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid SSTable encoding in " + file, e);
        }
    }

    /**
     * Reads all records.
     * Tombstones and expired values are represented by null values.
     */
    public NavigableMap<String, String> readAll() throws IOException {
        NavigableMap<String, String> entries = new TreeMap<>();

        for (Map.Entry<String, StorageRecord> entry
                : readAllRecords().entrySet()) {

            StorageRecord record = entry.getValue();

            if (record == null || record.isExpired()) {
                entries.put(entry.getKey(), null);
            } else {
                entries.put(entry.getKey(), record.getValue());
            }
        }

        return entries;
    }

    /**
     * Reads records while preserving their expiry metadata.
     * Supports TTL, tombstone, V:, and legacy record formats.
     */
    public NavigableMap<String, StorageRecord> readAllRecords()
            throws IOException {

        if (!Files.isRegularFile(file)) {
            throw new IOException("SSTable file not found: " + file);
        }

        NavigableMap<String, StorageRecord> records = new TreeMap<>();

        try (BufferedReader reader = Files.newBufferedReader(
                file, StandardCharsets.UTF_8)) {

            String line;

            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }

                String[] parts = line.split("\\t", -1);

                if (parts.length != 2) {
                    throw new IOException("Invalid SSTable record in " + file);
                }

                try {
                    String key = decode(parts[0]);
                    String encoded = parts[1];

                    if (encoded.startsWith("D:")) {
                        records.put(key, null);

                    } else if (encoded.startsWith("T:")) {
                        int separator = encoded.indexOf(':', 2);

                        if (separator < 0) {
                            throw new IOException(
                                    "Invalid TTL record in " + file);
                        }

                        long expiry = Long.parseLong(
                                encoded.substring(2, separator));

                        String value = decode(
                                encoded.substring(separator + 1));

                        records.put(
                                key, new StorageRecord(value, expiry));

                    } else if (encoded.startsWith("V:")) {
                        records.put(
                                key,
                                new StorageRecord(
                                        decode(encoded.substring(2)), -1L));

                    } else {
                        // Legacy SSTable format.
                        records.put(
                                key,
                                new StorageRecord(decode(encoded), -1L));
                    }

                } catch (IllegalArgumentException e) {
                    throw new IOException(
                            "Invalid SSTable encoding in " + file, e);
                }
            }
        }

        return records;
    }

    public Path getFile() {
        return file;
    }

    private static String encode(String value) {
        return Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(
                        value.getBytes(StandardCharsets.UTF_8));
    }

    private static String decode(String value) {
        return new String(
                Base64.getUrlDecoder().decode(value),
                StandardCharsets.UTF_8);
    }
}