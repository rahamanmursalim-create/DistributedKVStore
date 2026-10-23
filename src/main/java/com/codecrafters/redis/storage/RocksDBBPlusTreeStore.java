package com.codecrafters.redis.storage;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

/** Durable RocksDB LSM storage; point lookups and ordered scans use RocksDB directly. */
public final class RocksDBBPlusTreeStore implements AutoCloseable {
    private static final byte[] RECORD_MAGIC = {0x4b, 0x56, 0x01};
    static { RocksDB.loadLibrary(); }

    private final RocksDB database;
    private final Options options;
    private final WriteOptions writeOptions;
    private boolean closed;

    public RocksDBBPlusTreeStore(Path directory) throws IOException {
        Files.createDirectories(directory);
        options = new Options().setCreateIfMissing(true).setParanoidChecks(true);
        writeOptions = new WriteOptions().setSync(true).setDisableWAL(false);
        RocksDB opened = null;
        try {
            opened = RocksDB.open(options, directory.toAbsolutePath().toString());
            database = opened;
        } catch (RocksDBException | RuntimeException e) {
            if (opened != null) opened.close();
            writeOptions.close(); options.close();
            throw new IOException("Could not open RocksDB at " + directory, e);
        }
    }

    public synchronized void put(String key, String value) throws IOException {
        putEntry(key, new ValueEntry(value));
    }

    public synchronized void putEntry(String key, ValueEntry entry) throws IOException {
        ensureOpen(); requireKeyEntry(key, entry);
        try { database.put(writeOptions, key.getBytes(StandardCharsets.UTF_8), encode(entry)); }
        catch (RocksDBException e) { throw new IOException("RocksDB put failed", e); }
    }

    public synchronized String get(String key) throws IOException {
        ValueEntry entry = getEntry(key);
        return entry == null ? null : entry.getValue();
    }

    public synchronized ValueEntry getEntry(String key) throws IOException {
        ensureOpen(); if (key == null) throw new IllegalArgumentException("key cannot be null");
        ValueEntry entry;
        try {
            byte[] encoded = database.get(key.getBytes(StandardCharsets.UTF_8));
            entry = encoded == null ? null : decode(encoded);
        } catch (RocksDBException e) { throw new IOException("RocksDB get failed", e); }
        if (entry != null && entry.isExpired()) { delete(key); return null; }
        return entry;
    }

    public synchronized boolean delete(String key) throws IOException {
        ensureOpen(); if (key == null) throw new IllegalArgumentException("key cannot be null");
        boolean existed;
        try { existed = database.get(key.getBytes(StandardCharsets.UTF_8)) != null; }
        catch (RocksDBException e) { throw new IOException("RocksDB existence check failed", e); }
        try { database.delete(writeOptions, key.getBytes(StandardCharsets.UTF_8)); }
        catch (RocksDBException e) { throw new IOException("RocksDB delete failed", e); }
        return existed;
    }

    public synchronized void putBatch(Map<String, String> entries) throws IOException {
        Map<String, ValueEntry> values = new LinkedHashMap<>();
        if (entries == null) throw new IllegalArgumentException("entries cannot be null");
        entries.forEach((key, value) -> values.put(key, new ValueEntry(value)));
        applyBatch(values, java.util.List.of());
    }

    /** Atomically persists puts and deletes through RocksDB's write batch. */
    public synchronized void applyBatch(Map<String, ValueEntry> puts, Collection<String> deletes)
            throws IOException {
        ensureOpen();
        if (puts == null || deletes == null) throw new IllegalArgumentException("batch collections cannot be null");
        for (Map.Entry<String, ValueEntry> entry : puts.entrySet()) requireKeyEntry(entry.getKey(), entry.getValue());
        for (String key : deletes) if (key == null) throw new IllegalArgumentException("delete key cannot be null");
        try (WriteBatch batch = new WriteBatch()) {
            for (Map.Entry<String, ValueEntry> entry : puts.entrySet())
                batch.put(entry.getKey().getBytes(StandardCharsets.UTF_8), encode(entry.getValue()));
            for (String key : deletes) batch.delete(key.getBytes(StandardCharsets.UTF_8));
            database.write(writeOptions, batch);
        } catch (RocksDBException e) { throw new IOException("RocksDB batch write failed", e); }
    }

    /** Replaces all rows in one synced RocksDB write batch. */
    public synchronized void replaceAll(Map<String, ValueEntry> entries) throws IOException {
        ensureOpen(); if (entries == null) throw new IllegalArgumentException("entries cannot be null");
        try (WriteBatch batch = new WriteBatch(); RocksIterator iterator = database.newIterator()) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) batch.delete(iterator.key());
            iterator.status();
            for (Map.Entry<String, ValueEntry> entry : entries.entrySet()) {
                requireKeyEntry(entry.getKey(), entry.getValue());
                batch.put(entry.getKey().getBytes(StandardCharsets.UTF_8), encode(entry.getValue()));
            }
            database.write(writeOptions, batch);
        } catch (RocksDBException e) { throw new IOException("RocksDB snapshot replacement failed", e); }
    }

    public synchronized NavigableMap<String, String> snapshot() throws IOException {
        ensureOpen(); NavigableMap<String, String> result = new TreeMap<>();
        for (Map.Entry<String, ValueEntry> entry : entriesSnapshot().entrySet())
            if (!entry.getValue().isExpired()) result.put(entry.getKey(), entry.getValue().getValue());
        return java.util.Collections.unmodifiableNavigableMap(result);
    }

    public synchronized NavigableMap<String, ValueEntry> entriesSnapshot() throws IOException {
        ensureOpen(); NavigableMap<String, ValueEntry> result = new TreeMap<>();
        try (RocksIterator iterator = database.newIterator()) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next())
                result.put(new String(iterator.key(), StandardCharsets.UTF_8), decode(iterator.value()));
            iterator.status();
        } catch (RocksDBException e) { throw new IOException("RocksDB iteration failed", e); }
        return java.util.Collections.unmodifiableNavigableMap(result);
    }

    /** Visits rows in key order while retaining only RocksDB's iterator state. */
    public synchronized void forEachEntry(BiConsumer<String, ValueEntry> visitor) throws IOException {
        ensureOpen(); java.util.Objects.requireNonNull(visitor, "visitor");
        try (RocksIterator iterator = database.newIterator()) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next())
                visitor.accept(new String(iterator.key(), StandardCharsets.UTF_8), decode(iterator.value()));
            iterator.status();
        } catch (RocksDBException e) { throw new IOException("RocksDB iteration failed", e); }
    }

    /** Counts records with a streaming iterator instead of allocating a full key index. */
    public synchronized int countLiveEntries() throws IOException {
        ensureOpen(); int count = 0;
        try (RocksIterator iterator = database.newIterator()) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next())
                if (!decode(iterator.value()).isExpired()) count++;
            iterator.status();
        } catch (RocksDBException e) { throw new IOException("RocksDB iteration failed", e); }
        return count;
    }

    private static byte[] encode(ValueEntry entry) throws IOException {
        byte[] value = entry.getValue().getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(RECORD_MAGIC.length + 12 + value.length);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.write(RECORD_MAGIC); out.writeLong(entry.getExpiryTimestampMillis());
            out.writeInt(value.length); out.write(value);
        }
        return bytes.toByteArray();
    }

    private static ValueEntry decode(byte[] bytes) throws IOException {
        if (bytes.length < RECORD_MAGIC.length || bytes[0] != RECORD_MAGIC[0]
                || bytes[1] != RECORD_MAGIC[1] || bytes[2] != RECORD_MAGIC[2])
            return new ValueEntry(new String(bytes, StandardCharsets.UTF_8)); // legacy plain-string records
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            in.skipNBytes(RECORD_MAGIC.length); long expiry = in.readLong(); int length = in.readInt();
            if (expiry == 0 || length < 0 || length > bytes.length) throw new IOException("Invalid RocksDB value record");
            byte[] value = in.readNBytes(length);
            if (value.length != length || in.available() != 0) throw new IOException("Truncated RocksDB value record");
            return new ValueEntry(new String(value, StandardCharsets.UTF_8), expiry);
        }
    }

    private void ensureOpen() throws IOException { if (closed) throw new IOException("RocksDB store is closed"); }
    private static void requireKeyEntry(String key, ValueEntry entry) {
        if (key == null || entry == null || entry.getValue() == null)
            throw new IllegalArgumentException("key and value entry cannot be null");
    }

    @Override public synchronized void close() {
        if (closed) return;
        closed = true; database.close(); writeOptions.close(); options.close();
    }
}
