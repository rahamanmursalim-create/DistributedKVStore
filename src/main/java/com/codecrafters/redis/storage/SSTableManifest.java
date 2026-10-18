package com.codecrafters.redis.storage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Persists the active SSTable IDs for StorageEngine recovery.
 */
public final class SSTableManifest {

    private final Path file;

    public SSTableManifest(Path directory) {
        this.file = directory.resolve("MANIFEST");
    }

    public List<Long> load() throws IOException {
        if (!Files.exists(file)) {
            return null;
        }

        List<Long> ids = new ArrayList<>();
        java.util.Set<Long> uniqueIds = new java.util.HashSet<>();

        for (String line : Files.readAllLines(
                file, StandardCharsets.UTF_8)) {
            String value = line.trim();

            if (value.isEmpty()) {
                continue;
            }

            try {
                long id = Long.parseLong(value);

                if (id < 1) {
                    throw new IOException(
                            "Invalid SSTable ID in manifest: " + value);
                }

                if (!uniqueIds.add(id)) {
                    throw new IOException(
                            "Duplicate SSTable ID in manifest: " + id);
                }

                ids.add(id);
            } catch (NumberFormatException e) {
                throw new IOException(
                        "Invalid manifest entry: " + value, e);
            }
        }

        return ids;
    }

    public synchronized void save(List<Long> ids) throws IOException {
        if (ids == null) {
            throw new IllegalArgumentException("SSTable IDs must not be null");
        }
        java.util.Set<Long> uniqueIds = new java.util.HashSet<>();
        for (Long id : ids) {
            if (id == null || id < 1) {
                throw new IllegalArgumentException(
                        "SSTable IDs must be positive and non-null");
            }
            if (!uniqueIds.add(id)) {
                throw new IllegalArgumentException(
                        "Duplicate SSTable ID: " + id);
            }
        }

        Path temp = Files.createTempFile(
                file.getParent(), "MANIFEST-", ".tmp");

        boolean published = false;

        try {
            List<String> lines = ids.stream()
                    .map(String::valueOf)
                    .toList();

            Files.write(temp, lines, StandardCharsets.UTF_8);

            IOException lastEx = null;
            for (int attempt = 0; attempt < 10; attempt++) {
                try {
                    try {
                        Files.move(
                                temp, file,
                                StandardCopyOption.ATOMIC_MOVE,
                                StandardCopyOption.REPLACE_EXISTING);
                    } catch (java.nio.file.AtomicMoveNotSupportedException e) {
                        Files.move(
                                temp, file,
                                StandardCopyOption.REPLACE_EXISTING);
                    }
                    published = true;
                    break;
                } catch (IOException e) {
                    lastEx = e;
                    try {
                        Thread.sleep(15);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Interrupted while renaming manifest", ie);
                    }
                }
            }

            if (!published && lastEx != null) {
                throw lastEx;
            }
        } finally {
            if (!published) {
                Files.deleteIfExists(temp);
            }
        }
    }
}
