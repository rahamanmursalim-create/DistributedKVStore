package com.codecrafters.redis.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public final class StorageEngine implements AutoCloseable {

    private static final Pattern SSTABLE_NAME =
            Pattern.compile("sstable-(\\d+)\\.sst");

    private static final Pattern WAL_NAME =
            Pattern.compile("memtable-(\\d+)\\.wal");

    private final MemTable memTable = new MemTable();
    private final Path directory;
    private final SSTableManifest manifest;
    private final List<SSTable> ssTables = new ArrayList<>();

    private long nextTableId = 1;
    private WriteAheadLog wal;

    public StorageEngine(Path directory) throws IOException {
        this.directory = directory.toAbsolutePath().normalize();

        Files.createDirectories(this.directory);
        this.manifest = new SSTableManifest(this.directory);

        loadExistingSSTables();
        recoverFromWalFiles();

        this.wal = new WriteAheadLog(walPath(nextTableId));
    }

    private Path walPath(long id) {
        return directory.resolve(
                String.format("memtable-%020d.wal", id));
    }

    private Path sstablePath(long id) {
        return directory.resolve(
                String.format("sstable-%020d.sst", id));
    }

    private void loadExistingSSTables() throws IOException {
        List<Long> ids = manifest.load();

        if (ids == null) {
            // Migrate an existing directory that predates MANIFEST.
            try (Stream<Path> paths = Files.list(directory)) {
                ids = paths
                        .filter(Files::isRegularFile)
                        .filter(path -> SSTABLE_NAME.matcher(
                                path.getFileName().toString()).matches())
                        .map(StorageEngine::extractTableId)
                        .sorted()
                        .toList();
            }

            manifest.save(ids);
        }

        Set<Long> seen = new HashSet<>();
        long previousId = 0;

        for (long id : ids) {
            if (id <= previousId || !seen.add(id)) {
                throw new IOException(
                        "Manifest SSTable IDs must be unique and sorted");
            }

            Path file = sstablePath(id);

            if (!Files.isRegularFile(file)) {
                throw new IOException(
                        "Manifest references missing SSTable: " + file);
            }

            SSTable table = SSTable.open(file);
            table.readAll();

            ssTables.add(table);
            nextTableId = Math.max(nextTableId, id + 1);
            previousId = id;
        }

        // Unreferenced SSTables are ignored. They may be leftovers
        // from an interrupted flush or compaction.
    }

    private void recoverFromWalFiles() throws IOException {
        List<Path> walFiles;

        try (Stream<Path> paths = Files.list(directory)) {
            walFiles = paths
                    .filter(Files::isRegularFile)
                    .filter(path -> WAL_NAME.matcher(
                            path.getFileName().toString()).matches())
                    .sorted(Comparator.comparingLong(
                            StorageEngine::extractWalId))
                    .toList();
        }

        for (Path path : walFiles) {
            long id = extractWalId(path);

            boolean activeTableExists = ssTables.stream()
                    .anyMatch(table ->
                            extractTableId(table.getFile()) == id);

            // A WAL is obsolete only when its matching SSTable is active.
            if (activeTableExists && id < nextTableId) {
                Files.deleteIfExists(path);
                continue;
            }

            if (id < nextTableId) {
                throw new IOException(
                        "Obsolete WAL is not represented by the manifest: "
                                + path);
            }

            if (id != nextTableId) {
                throw new IOException(
                        "Unexpected WAL generation: " + path);
            }

            try (WriteAheadLog recoveryWal = new WriteAheadLog(path)) {
                for (WriteAheadLog.Record record : recoveryWal.replay()) {
                    if (record.getOperation()
                            == WriteAheadLog.Operation.PUT) {
                        memTable.put(
                                record.getKey(), record.getValue());
                    } else if (record.getOperation()
                            == WriteAheadLog.Operation.DELETE) {
                        memTable.delete(record.getKey());
                    }
                }
            }
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

    private static long extractWalId(Path file) {
        Matcher matcher = WAL_NAME.matcher(
                file.getFileName().toString());

        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                    "Invalid WAL filename: " + file);
        }

        return Long.parseLong(matcher.group(1));
    }

    private List<Long> activeTableIds() {
        return ssTables.stream()
                .map(table -> extractTableId(table.getFile()))
                .sorted()
                .toList();
    }

    public synchronized void put(String key, String value)
            throws IOException {
        ensureOpen();

        if (key == null || value == null) {
            throw new IllegalArgumentException(
                    "Key and value cannot be null");
        }

        wal.appendPut(key, value, -1L);
        memTable.put(key, value);
    }

    public synchronized String get(String key) throws IOException {
        if (key == null) {
            return null;
        }

        if (memTable.containsKey(key)) {
            return memTable.get(key);
        }

        for (int i = ssTables.size() - 1; i >= 0; i--) {
            SSTable table = ssTables.get(i);

            if (table.containsKey(key)) {
                return table.get(key);
            }
        }

        return null;
    }

    public synchronized boolean delete(String key)
            throws IOException {
        ensureOpen();

        if (key == null) {
            throw new IllegalArgumentException(
                    "Key cannot be null");
        }

        String previousValue = get(key);

        wal.appendDelete(key);
        memTable.delete(key);

        return previousValue != null;
    }

    public synchronized void flush() throws IOException {
        ensureOpen();

        if (memTable.isEmpty()) {
            return;
        }

        long flushedId = nextTableId;
        Path tablePath = sstablePath(flushedId);
        Map<String, String> entries = memTable.snapshot();

        // Write the immutable SSTable first.
        SSTable written = SSTable.write(tablePath, entries);

        List<Long> newIds = new ArrayList<>(activeTableIds());
        newIds.add(flushedId);
        newIds.sort(Long::compareTo);

        // Publish the new active set before changing in-memory state.
        try {
            manifest.save(newIds);
        } catch (IOException e) {
            Files.deleteIfExists(tablePath);
            throw e;
        }

        ssTables.add(written);
        ssTables.sort(Comparator.comparingLong(
                table -> extractTableId(table.getFile())));

        // The SSTable now contains all entries from this WAL generation.
        wal.close();
        wal = null;
        memTable.clear();
        nextTableId++;

        Files.deleteIfExists(walPath(flushedId));
        wal = new WriteAheadLog(walPath(nextTableId));
    }

    /**
     * Merges all active SSTables into one sorted SSTable.
     * Pending MemTable writes are flushed before compaction.
     */
    public synchronized void compact() throws IOException {
        ensureOpen();

        if (ssTables.size() < 2) {
            return;
        }

        // CRITICAL: persist pending writes before changing table IDs.
        // This prevents unflushed WAL data from being mistaken for
        // an obsolete WAL after compaction and restart.
        if (!memTable.isEmpty()) {
            flush();
        }

        NavigableMap<String, String> merged = new TreeMap<>();

        // Older tables first; newer values take precedence.
        for (SSTable table : ssTables) {
            merged.putAll(table.readAll());
        }

        long compactedId = nextTableId;
        Path compactedPath = sstablePath(compactedId);

        List<SSTable> oldTables = new ArrayList<>(ssTables);
        List<Long> newIds = List.of(compactedId);

        // This ID is being reused for the compacted SSTable.
        // Close its empty WAL before publishing the new table.
        wal.close();
        wal = null;

        boolean published = false;

        try {
            SSTable compacted = SSTable.write(compactedPath, merged);

            // Publish the new active set before deleting old SSTables.
            manifest.save(newIds);
            published = true;

            ssTables.clear();
            ssTables.add(compacted);
            nextTableId++;

            // The next WAL must have a different ID from the new SSTable.
            wal = new WriteAheadLog(walPath(nextTableId));

            // Delete old tables only after manifest publication.
            for (SSTable oldTable : oldTables) {
                Files.deleteIfExists(oldTable.getFile());
            }

        } catch (IOException e) {
            if (!published) {
                try {
                    Files.deleteIfExists(compactedPath);
                } catch (IOException cleanupError) {
                    e.addSuppressed(cleanupError);
                }
            }

            // Try to restore a usable WAL if opening it failed.
            if (wal == null) {
                try {
                    wal = new WriteAheadLog(walPath(nextTableId));
                } catch (IOException reopenError) {
                    e.addSuppressed(reopenError);
                }
            }

            throw e;
        }
    }

    public synchronized int memTableSize() {
        return memTable.size();
    }

    public synchronized int sstableCount() {
        return ssTables.size();
    }

    public synchronized NavigableMap<String, String> snapshot()
            throws IOException {
        NavigableMap<String, String> result = new TreeMap<>();

        for (SSTable table : ssTables) {
            applyEntries(result, table.readAll());
        }

        applyEntries(result, memTable.snapshot());
        return result;
    }

    private static void applyEntries(
            NavigableMap<String, String> target,
            Map<String, String> entries) {

        for (Map.Entry<String, String> entry : entries.entrySet()) {
            if (entry.getValue() == null) {
                target.remove(entry.getKey());
            } else {
                target.put(entry.getKey(), entry.getValue());
            }
        }
    }

    private void ensureOpen() throws IOException {
        if (wal == null) {
            throw new IOException("StorageEngine is closed");
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (wal != null) {
            wal.close();
            wal = null;
        }
    }
}