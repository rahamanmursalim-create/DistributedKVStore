package com.codecrafters.redis.storage;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Thread-safe key-value store with TTL and selectable WAL or RocksDB persistence.
 *
 * The WAL is written and synced before an in-memory mutation is published.
 * Default WAL location: ./data/wal.log (relative to the process working directory).
 */
public class RedisStore implements AutoCloseable, KeyValueAccess {
    private final BPlusTree<ValueEntry> store = new BPlusTree<>();
    private final Map<String, Long> keyVersions = new HashMap<>();
    private long commitVersion;
    private final ScheduledExecutorService cleanupExecutor;
    private final WriteAheadLog wal;
    private final RocksDBBPlusTreeStore rocksDb;

    public RedisStore() {
        this(true);
    }

    public RedisStore(boolean enableBackgroundCleanup) {
        this(Path.of("data", "wal.log"), enableBackgroundCleanup);
    }

    public RedisStore(Path walPath, boolean enableBackgroundCleanup) {
        this(openWal(walPath), null, enableBackgroundCleanup);
    }

    public static RedisStore usingRocksDB(Path directory, boolean enableBackgroundCleanup) {
        try { return new RedisStore(null, new RocksDBBPlusTreeStore(directory), enableBackgroundCleanup); }
        catch (IOException e) { throw new UncheckedIOException("Could not open RocksDB at " + directory, e); }
    }

    private RedisStore(WriteAheadLog wal, RocksDBBPlusTreeStore rocksDb, boolean enableBackgroundCleanup) {
        this.wal = wal;
        this.rocksDb = rocksDb;
        try {
            recoverFromStorage();
        } catch (IOException e) {
            if (wal != null) try { wal.close(); } catch (IOException ignored) { }
            if (rocksDb != null) rocksDb.close();
            throw new UncheckedIOException("Could not recover storage", e);
        }

        if (enableBackgroundCleanup) {
            this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "redis-active-expiration");
                t.setDaemon(true);
                return t;
            });
            this.cleanupExecutor.scheduleAtFixedRate(
                    this::cleanupExpiredKeys, 100, 100, TimeUnit.MILLISECONDS);
        } else {
            this.cleanupExecutor = null;
        }
    }

    private static WriteAheadLog openWal(Path walPath) {
        try { return new WriteAheadLog(walPath); }
        catch (IOException e) { throw new UncheckedIOException("Could not open WAL at " + walPath, e); }
    }

    private void recoverFromStorage() throws IOException {
        long now = System.currentTimeMillis();
        if (rocksDb != null) {
            for (Map.Entry<String, ValueEntry> entry : rocksDb.entriesSnapshot().entrySet()) {
                commitVersion++;
                keyVersions.put(entry.getKey(), commitVersion);
                if (entry.getValue().isExpired()) rocksDb.delete(entry.getKey());
                else store.put(entry.getKey(), entry.getValue());
            }
            return;
        }
        for (WriteAheadLog.Record record : wal.replay()) {
            commitVersion++;
            keyVersions.put(record.getKey(), commitVersion);
            if (record.getOperation() == WriteAheadLog.Operation.DELETE) {
                store.remove(record.getKey());
            } else {
                long expiry = record.getExpiryEpochMillis();
                if (expiry >= 0 && expiry <= now) {
                    store.remove(record.getKey());
                } else if (expiry < 0) {
                    store.put(record.getKey(), new ValueEntry(record.getValue()));
                } else {
                    store.put(record.getKey(),
                            new ValueEntry(record.getValue(), expiry));
                }
            }
        }
    }

    /** Stores a key without expiration. */
    public synchronized void set(String key, String value) {
        appendPut(key, value, -1L);
        store.put(key, new ValueEntry(value));
        recordMutation(key);
    }

    /** Durably writes a batch with one WAL force, then publishes all values. */
    public synchronized void setBatch(Map<String, String> values) {
        if (values == null || values.isEmpty()) return;
        List<WriteAheadLog.PutRecord> records = new ArrayList<>(values.size());
        for (Map.Entry<String, String> entry : values.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null)
                throw new IllegalArgumentException("Key and value cannot be null");
            records.add(new WriteAheadLog.PutRecord(entry.getKey(), entry.getValue(), -1L));
        }
        try {
            if (rocksDb != null) {
                Map<String, ValueEntry> puts = new java.util.LinkedHashMap<>();
                values.forEach((key, value) -> puts.put(key, new ValueEntry(value)));
                rocksDb.applyBatch(puts, List.of());
            } else wal.appendPuts(records);
        } catch (IOException e) { throw new UncheckedIOException("Could not persist PUT batch", e); }
        for (Map.Entry<String, String> entry : values.entrySet())
            store.put(entry.getKey(), new ValueEntry(entry.getValue()));
        recordMutations(values.keySet());
    }

    /** Stores a key with TTL relative to now, in milliseconds. */
    public synchronized void set(String key, String value, long expiryMillis) {
        if (expiryMillis <= 0) {
            del(key);
            return;
        }
        long expiryTimestamp = Math.addExact(System.currentTimeMillis(), expiryMillis);
        appendPut(key, value, expiryTimestamp);
        store.put(key, new ValueEntry(value, expiryTimestamp));
        recordMutation(key);
    }

    /** Stores a key with an absolute Unix expiry timestamp in milliseconds. */
    public synchronized void setWithAbsoluteExpiry(
            String key, String value, long expiryTimestampMillis) {
        if (expiryTimestampMillis <= System.currentTimeMillis()) {
            del(key);
            return;
        }
        appendPut(key, value, expiryTimestampMillis);
        store.put(key, new ValueEntry(value, expiryTimestampMillis));
        recordMutation(key);
    }

    private void appendPut(String key, String value, long expiryTimestamp) {
        try {
            if (rocksDb != null) rocksDb.putEntry(key, new ValueEntry(value, expiryTimestamp));
            else wal.appendPut(key, value, expiryTimestamp);
        } catch (IOException e) {
            throw new UncheckedIOException("Could not persist PUT for key " + key, e);
        }
    }

    public String get(String key) {
        ValueEntry entry = store.get(key);
        if (entry == null) {
            return null;
        }
        if (entry.isExpired()) {
            store.removeIfSame(key, entry);
            // The durable value retains its absolute expiry; avoid deleting a concurrent replacement.
            return null;
        }
        return entry.getValue();
    }

    public ValueEntry getEntry(String key) {
        ValueEntry entry = store.get(key);
        if (entry == null) {
            return null;
        }
        if (entry.isExpired()) {
            store.removeIfSame(key, entry);
            // The durable value retains its absolute expiry; avoid deleting a concurrent replacement.
            return null;
        }
        return entry;
    }

    public synchronized boolean exists(String key) {
        return get(key) != null;
    }

    public synchronized int del(String... keys) {
        int count = 0;
        for (String key : keys) {
            // Record the delete even when the key is absent, so an older PUT
            // cannot be resurrected during replay.
            appendDelete(key);
            ValueEntry removed = store.remove(key);
            recordMutation(key);
            if (removed != null && !removed.isExpired()) {
                count++;
            }
        }
        return count;
    }

    /** Starts a repeatable-read snapshot transaction. */
    public synchronized MVCCTransaction beginTransaction() {
        Map<String, String> snapshot = new HashMap<>();
        for (Map.Entry<String, ValueEntry> entry : store.snapshot().entrySet()) {
            ValueEntry value = entry.getValue();
            if (value.isExpired()) {
                store.remove(entry.getKey());
                deleteExpiredFromRocksDb(entry.getKey());
            } else {
                snapshot.put(entry.getKey(), value.getValue());
            }
        }
        return new MVCCTransaction(this, commitVersion, Map.copyOf(snapshot));
    }

    synchronized void commitTransaction(long snapshotVersion, Map<String, String> changes) {
        for (String key : changes.keySet()) {
            if (keyVersions.getOrDefault(key, 0L) > snapshotVersion) {
                throw new TransactionConflictException(key);
            }
        }
        if (changes.isEmpty()) return;
        try {
            if (rocksDb != null) {
                Map<String, ValueEntry> puts = new java.util.LinkedHashMap<>();
                List<String> deletes = new ArrayList<>();
                changes.forEach((key, value) -> {
                    if (value == null) deletes.add(key); else puts.put(key, new ValueEntry(value));
                });
                rocksDb.applyBatch(puts, deletes);
            } else wal.appendTransaction(changes);
        } catch (IOException e) { throw new UncheckedIOException("Could not persist transaction", e); }

        long transactionVersion = ++commitVersion;
        for (Map.Entry<String, String> change : changes.entrySet()) {
            keyVersions.put(change.getKey(), transactionVersion);
            if (change.getValue() == null) store.remove(change.getKey());
            else store.put(change.getKey(), new ValueEntry(change.getValue()));
        }
    }

    private void recordMutation(String key) {
        keyVersions.put(key, ++commitVersion);
    }

    private void recordMutations(java.util.Collection<String> keys) {
        long version = ++commitVersion;
        for (String key : keys) keyVersions.put(key, version);
    }

    public synchronized List<String> keys(String pattern) {
        Pattern regex = globToRegex(pattern);
        List<String> matchedKeys = new ArrayList<>();
        long now = System.currentTimeMillis();

        for (Map.Entry<String, ValueEntry> e : store.snapshot().entrySet()) {
            ValueEntry entry = e.getValue();
            if (entry.hasExpiry() && entry.getExpiryTimestampMillis() <= now) {
                store.remove(e.getKey());
                deleteExpiredFromRocksDb(e.getKey());
                continue;
            }
            if (regex.matcher(e.getKey()).matches()) {
                matchedKeys.add(e.getKey());
            }
        }
        return matchedKeys;
    }

    public synchronized void cleanupExpiredKeys() {
        long now = System.currentTimeMillis();
        for (Map.Entry<String, ValueEntry> entry : store.snapshot().entrySet()) {
            ValueEntry value = entry.getValue();
            if (value.hasExpiry() && value.getExpiryTimestampMillis() <= now) {
                store.remove(entry.getKey());
                deleteExpiredFromRocksDb(entry.getKey());
            }
        }
    }

    public synchronized int size() {
        cleanupExpiredKeys();
        return store.size();
    }

    public synchronized void clear() {
        del(store.snapshot().keySet().toArray(new String[0]));
    }

    private static Pattern globToRegex(String glob) {
        if (glob == null || glob.equals("*")) {
            return Pattern.compile(".*");
        }
        StringBuilder sb = new StringBuilder("^");
        for (int i = 0; i < glob.length(); i++) {
            char c = glob.charAt(i);
            switch (c) {
                case '*': sb.append(".*"); break;
                case '?': sb.append('.'); break;
                case '.':
                case '(':
                case ')':
                case '+':
                case '|':
                case '^':
                case '$':
                case '@':
                case '%':
                case '\\':
                    sb.append('\\').append(c);
                    break;
                default:
                    sb.append(c);
            }
        }
        sb.append('$');
        return Pattern.compile(sb.toString());
    }

    @Override
    public void close() {
        if (cleanupExecutor != null) {
            cleanupExecutor.shutdown();
            try {
                if (!cleanupExecutor.awaitTermination(500, TimeUnit.MILLISECONDS)) {
                    cleanupExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                cleanupExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        if (rocksDb != null) rocksDb.close();
        else try { wal.close(); }
        catch (IOException e) { throw new UncheckedIOException("Could not close WAL", e); }
    }

    private void appendDelete(String key) {
        try {
            if (rocksDb != null) rocksDb.delete(key); else wal.appendDelete(key);
        } catch (IOException e) { throw new UncheckedIOException("Could not persist DELETE for key " + key, e); }
    }

    private void deleteExpiredFromRocksDb(String key) {
        if (rocksDb != null) appendDelete(key);
    }
}
