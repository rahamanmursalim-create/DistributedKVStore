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

public final class StorageEngine implements AutoCloseable {

    private static final Pattern SSTABLE_NAME =
            Pattern.compile("sstable-(\\d+)\\.sst");

    private static final Pattern WAL_NAME =
            Pattern.compile("memtable-(\\d+)\\.wal");

    private final MemTable memTable = new MemTable();
    private final Path directory;
    private final List<SSTable> ssTables = new ArrayList<>();

    private long nextTableId = 1;
    private WriteAheadLog wal;

    public StorageEngine(Path directory) throws IOException {
        this.directory = directory.toAbsolutePath().normalize();

        Files.createDirectories(this.directory);
        loadExistingSSTables();
        recoverFromWalFiles();

        this.wal = new WriteAheadLog(walPath(nextTableId));
    }

    private Path walPath(long id) {
        return directory.resolve(
                String.format("memtable-%020d.wal", id));
    }

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
            table.readAll();

            ssTables.add(table);
            nextTableId = Math.max(nextTableId, id + 1);
        }
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

            // A published SSTable with the same ID means
            // this WAL's flush already completed.
            if (id < nextTableId) {
    Path correspondingSSTable = directory.resolve(
            String.format("sstable-%020d.sst", id));

    if (Files.exists(correspondingSSTable)) {
        Files.deleteIfExists(path);
        continue;
    }

    throw new IOException(
            "WAL exists without its corresponding SSTable: " + path);
}

            if (id != nextTableId) {
                throw new IOException(
                        "Unexpected WAL generation: " + path);
            }

            try (WriteAheadLog recoveryWal =
                         new WriteAheadLog(path)) {
                for (WriteAheadLog.Record record :
                        recoveryWal.replay()) {

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

    public synchronized String get(String key)
            throws IOException {
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

        Path tablePath = directory.resolve(
                String.format("sstable-%020d.sst", flushedId));

        Map<String, String> entries = memTable.snapshot();

        // WAL stays intact until the SSTable is published.
        SSTable written = SSTable.write(tablePath, entries);
        ssTables.add(written);

        wal.close();
        memTable.clear();
        nextTableId++;

        // If deletion fails, recovery recognizes the matching
        // SSTable and will not replay this WAL.
        Files.deleteIfExists(walPath(flushedId));

        wal = new WriteAheadLog(walPath(nextTableId));
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

        for (Map.Entry<String, String> entry :
                entries.entrySet()) {
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