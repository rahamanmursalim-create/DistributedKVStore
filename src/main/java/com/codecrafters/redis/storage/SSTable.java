
package com.codecrafters.redis.storage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Immutable, sorted on-disk table for the LSM storage engine.
 *
 * Record format:
 * encodedKey<TAB>D:       -> deletion tombstone
 * encodedKey<TAB>V:value  -> normal value
 *
 * Legacy records containing an unmarked Base64 value
 * are also supported.
 */
public final class SSTable {

    private final Path file;
    private volatile NavigableMap<String, Long> keyOffsets;
    private volatile BloomFilter bloomFilter;

    private SSTable(Path file) {
        this.file = file.toAbsolutePath().normalize();
    }

    /**
     * Writes sorted entries to an SSTable.
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
                Files.move(temp, absolute,
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
            throw new IOException(
                    "SSTable file not found: " + absolute);
        }

        return new SSTable(absolute);
    }

    /**
     * Returns the value for a key.
     * Returns null if the key is absent or is a tombstone.
     *
     * Use containsKey() to distinguish these cases.
     */
    public String get(String key) throws IOException {
        if (key == null || !containsKey(key)) {
            return null;
        }

        long offset = keyOffsets.get(key);
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {
            raf.seek(offset);
            return parseRecord(raf.readLine()).getValue();
        }
    }

    /**
     * Checks whether a key has a record in this SSTable.
     * Returns true for tombstones as well.
     */
    public boolean containsKey(String key) throws IOException {
        if (key == null) {
            return false;
        }
        ensureIndex();
        if (!bloomFilter.mightContain(key)) {
            return false;
        }
        return keyOffsets.containsKey(key);
    }

    private void ensureIndex() throws IOException {
        if (keyOffsets != null) {
            return;
        }

        synchronized (this) {
            if (keyOffsets != null) {
                return;
            }

            NavigableMap<String, Long> index = new TreeMap<>();

            if (!Files.isRegularFile(file)) {
                throw new IOException("SSTable file not found: " + file);
            }

            try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {
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
                    index.put(record.getKey(), offset);
                }
            }

            keyOffsets = index;
            bloomFilter = new BloomFilter(index.keySet());
        }
    }

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
            } else if (encodedValue.startsWith("V:")) {
                value = decode(encodedValue.substring(2));
            } else {
                value = decode(encodedValue);
            }

            return new java.util.AbstractMap.SimpleImmutableEntry<>(key, value);
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid SSTable encoding in " + file, e);
        }
    }

    /**
     * Reads all records.
     * Tombstones are represented by null values in the map.
     */
    public NavigableMap<String, String> readAll()
            throws IOException {

        NavigableMap<String, String> entries =
                new TreeMap<>();

        if (!Files.isRegularFile(file)) {
            throw new IOException(
                    "SSTable file not found: " + file);
        }

        try (BufferedReader reader = Files.newBufferedReader(
                file,
                StandardCharsets.UTF_8)) {

            String line;

            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }

                String[] parts = line.split("\t", -1);

                if (parts.length != 2) {
                    throw new IOException(
                            "Invalid SSTable record in " + file);
                }

                try {
                    String key = decode(parts[0]);
                    String encodedValue = parts[1];
                    String value;

                    if (encodedValue.startsWith("D:")) {
                        // Deletion tombstone.
                        value = null;

                    } else if (encodedValue.startsWith("V:")) {
                        // New-format normal value.
                        value = decode(
                                encodedValue.substring(2));

                    } else {
                        // Backward compatibility with old SSTables.
                        value = decode(encodedValue);
                    }

                    entries.put(key, value);

                } catch (IllegalArgumentException e) {
                    throw new IOException(
                            "Invalid SSTable encoding in " + file,
                            e);
                }
            }
        }

        return entries;
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
