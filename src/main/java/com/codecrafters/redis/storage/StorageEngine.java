
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
            // Migrate a directory created before MANIFEST existed.
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

            // Validate records, including TTL metadata.
            table.readAllRecords();

            ssTables.add(table);
            nextTableId = Math.max(nextTableId, id + 1);
            previousId = id;
        }

        // Unreferenced SSTables may be leftovers from an interrupted
        // flush or compaction. They are not considered active.
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

            // An active SSTable makes its WAL obsolete.
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

            try (WriteAheadLog recoveryWal =
                         new WriteAheadLog(path)) {

                for (WriteAheadLog.Record record : recoveryWal.replay()) {
                    if (record.getOperation()
                            == WriteAheadLog.Operation.PUT) {

                        memTable.put(
                                record.getKey(),
                                record.getValue(),
                                record.getExpiryEpochMillis());

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

    public synchronized int activeTableCount() {
        return ssTables.size();
    }

    public synchronized void put(String key, String value)
            throws IOException {
        put(key, value, -1L);
    }

    /**
     * Stores a value with an absolute expiry timestamp in milliseconds.
     * Use -1 for a value that never expires.
     */
    public synchronized void put(
            String key,
            String value,
            long expiryEpochMillis) throws IOException {

        ensureOpen();

        if (key == null || value == null) {
            throw new IllegalArgumentException(
                    "Key and value cannot be null");
        }

        if (expiryEpochMillis < -1L) {
            throw new IllegalArgumentException(
                    "Invalid expiry timestamp");
        }

        // Persist the write before publishing it to the MemTable.
        wal.appendPut(key, value, expiryEpochMillis);
        memTable.put(key, value, expiryEpochMillis);
    }

    /**
     * Retrieves a key using targeted SSTable lookups.
     *
     * The MemTable is checked first. SSTables are searched newest-first.
     * A tombstone or expired record in a newer table must shadow any
     * older value for the same key.
     */
    public synchronized String get(String key) throws IOException {
        if (key == null) {
            return null;
        }

        // A MemTable entry, including a tombstone, overrides SSTables.
        if (memTable.containsKey(key)) {
            return memTable.get(key);
        }

        // Search newer SSTables before older SSTables.
        for (int i = ssTables.size() - 1; i >= 0; i--) {
            SSTable table = ssTables.get(i);

            Map.Entry<String, String> record = table.lookup(key);

            if (record != null) {
                // A null value means deleted or expired.
                // Return immediately so an older value cannot reappear.
                return record.getValue();
            }
        }

        return null;
    }

    public synchronized boolean delete(String key) throws IOException {
        ensureOpen();

        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        String previousValue = get(key);

        // Always persist the tombstone, even when the key is absent.
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

        Map<String, StorageRecord> entries =
                memTable.snapshotRecords();

        // Write the immutable SSTable first.
        SSTable written = SSTable.writeRecords(tablePath, entries);

        List<Long> newIds = new ArrayList<>(activeTableIds());
        newIds.add(flushedId);
        newIds.sort(Long::compareTo);

        // Publish the active table set before changing in-memory state.
        try {
            manifest.save(newIds);
        } catch (IOException e) {
            Files.deleteIfExists(tablePath);
            throw e;
        }

        ssTables.add(written);
        ssTables.sort(Comparator.comparingLong(
                table -> extractTableId(table.getFile())));

        // The SSTable now contains this WAL generation's entries.
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
        compactBatch(Integer.MAX_VALUE);
    }

    /**
     * Executes size-tiered multi-generation compaction if the number of active SSTables
     * meets or exceeds the specified threshold. Compacts up to maxTables in the batch.
     *
     * @param threshold Minimum number of active SSTables required to trigger compaction.
     * @param maxTables Maximum number of SSTables to compact in this batch.
     * @return true if compaction was executed, false if table count is below threshold.
     */
    public synchronized boolean sizeTieredCompact(int threshold, int maxTables) throws IOException {
        ensureOpen();

        if (threshold < 2) {
            throw new IllegalArgumentException("Compaction threshold must be at least 2");
        }

        if (ssTables.size() < threshold) {
            return false;
        }

        compactBatch(maxTables);
        return true;
    }

    /**
     * Compacts up to maxTables of the most recent SSTables into a single table.
     * If maxTables >= total tables, all active SSTables are merged into one.
     */
    public synchronized void compactBatch(int maxTables) throws IOException {
        ensureOpen();

        if (ssTables.size() < 2 || maxTables < 2) {
            return;
        }

        // Persist pending writes before changing table IDs.
        if (!memTable.isEmpty()) {
            flush();
        }

        if (ssTables.size() < 2) {
            return;
        }

        int count = Math.min(ssTables.size(), maxTables);
        if (count < 2) {
            return;
        }

        int startIndex = ssTables.size() - count;
        List<SSTable> tablesToCompact = new ArrayList<>(ssTables.subList(startIndex, ssTables.size()));
        List<SSTable> remainingTables = new ArrayList<>(ssTables.subList(0, startIndex));

        NavigableMap<String, StorageRecord> merged = new TreeMap<>();

        // Merge oldest to newest so newer records override older records within the batch.
        for (SSTable table : tablesToCompact) {
            merged.putAll(table.readAllRecords());
        }

        // Convert expired values into tombstones.
        for (Map.Entry<String, StorageRecord> entry : merged.entrySet()) {
            StorageRecord record = entry.getValue();
            if (record != null && record.isExpired()) {
                entry.setValue(null);
            }
        }

        long compactedId = nextTableId;
        Path compactedPath = sstablePath(compactedId);

        List<Long> newIds = new ArrayList<>();
        for (SSTable remaining : remainingTables) {
            newIds.add(extractTableId(remaining.getFile()));
        }
        newIds.add(compactedId);

        // Close the empty WAL before reusing its generation.
        wal.close();
        wal = null;

        boolean published = false;

        try {
            SSTable compacted = SSTable.writeRecords(compactedPath, merged);

            // Publish the new active set before deleting old SSTables.
            manifest.save(newIds);
            published = true;

            ssTables.clear();
            ssTables.addAll(remainingTables);
            ssTables.add(compacted);
            nextTableId++;

            // Create a new WAL generation for subsequent writes.
            wal = new WriteAheadLog(walPath(nextTableId));

            // Delete old tables only after manifest publication.
            for (SSTable oldTable : tablesToCompact) {
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

    /**
     * Returns lookup statistics for each active SSTable.
     * Intended for diagnostics and benchmarking.
     */
    public synchronized String getSSTableLookupStats() {
        StringBuilder stats = new StringBuilder();

        for (int i = ssTables.size() - 1; i >= 0; i--) {
            SSTable table = ssTables.get(i);

            stats.append(table.getFile().getFileName())
                    .append(": ")
                    .append(table.getLookupStats())
                    .append(System.lineSeparator());
        }

        return stats.toString();
    }

    /**
     * Returns all currently live key-value pairs in sorted key order.
     */
    public synchronized NavigableMap<String, String> snapshot()
            throws IOException {

        NavigableMap<String, String> result = new TreeMap<>();

        // Apply tables oldest to newest so newer records override older ones.
        for (SSTable table : ssTables) {
            applyRecordEntries(result, table.readAllRecords());
        }

        // MemTable entries are the newest records.
        applyRecordEntries(result, memTable.snapshotRecords());

        return result;
    }

    private static void applyRecordEntries(
            NavigableMap<String, String> target,
            Map<String, StorageRecord> entries) {

        for (Map.Entry<String, StorageRecord> entry
                : entries.entrySet()) {

            StorageRecord record = entry.getValue();

            if (record == null || record.isExpired()) {
                target.remove(entry.getKey());
            } else {
                target.put(entry.getKey(), record.getValue());
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
