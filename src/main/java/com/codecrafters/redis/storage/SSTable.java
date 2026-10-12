
package com.codecrafters.redis.storage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Immutable, sorted on-disk table for the initial LSM storage engine.
 * Each line contains a Base64-encoded key and value separated by a tab.
 */
public final class SSTable {

    private final Path file;

    private SSTable(Path file) {
        this.file = file.toAbsolutePath().normalize();
    }

    public static SSTable write(Path file, Map<String, String> entries)
            throws IOException {

        Path absolute = file.toAbsolutePath().normalize();
        Path parent = absolute.getParent();

        if (parent != null) {
            Files.createDirectories(parent);
        }

        NavigableMap<String, String> sorted = new TreeMap<>(entries);

        try (BufferedWriter writer = Files.newBufferedWriter(
                absolute,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE)) {

            for (Map.Entry<String, String> entry : sorted.entrySet()) {
                String key = encode(entry.getKey());
                String value = encode(entry.getValue());

                writer.write(key);
                writer.write('\t');
                writer.write(value);
                writer.newLine();
            }
        }

        return new SSTable(absolute);
    }

    public String get(String key) throws IOException {
        return readAll().get(key);
    }

    public NavigableMap<String, String> readAll() throws IOException {
        NavigableMap<String, String> entries = new TreeMap<>();

        if (!Files.exists(file)) {
            return entries;
        }

        try (BufferedReader reader = Files.newBufferedReader(
                file, StandardCharsets.UTF_8)) {

            String line;

            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }

                String[] parts = line.split("\t", -1);

                if (parts.length != 2) {
                    throw new IOException("Invalid SSTable record");
                }

                try {
                    String key = decode(parts[0]);
                    String value = decode(parts[1]);
                    entries.put(key, value);
                } catch (IllegalArgumentException e) {
                    throw new IOException("Invalid SSTable encoding", e);
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
                .encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String decode(String value) {
        return new String(
                Base64.getUrlDecoder().decode(value),
                StandardCharsets.UTF_8);
    }
}
