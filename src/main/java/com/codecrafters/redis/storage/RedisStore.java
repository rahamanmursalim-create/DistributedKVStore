package com.codecrafters.redis.storage;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.PriorityQueue;
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
    private static final int MAX_LIVE_KEYS = positiveIntSetting("KV_MAX_LIVE_KEYS", 100_000);
    private static final long MAX_LIVE_BYTES = positiveLongSetting("KV_MAX_LIVE_BYTES", 64L * 1024 * 1024);
    private static final int MAX_KEY_BYTES = positiveIntSetting("KV_MAX_KEY_BYTES", 1024);
    private static final int MAX_VALUE_BYTES = positiveIntSetting("KV_MAX_VALUE_BYTES", 1024 * 1024);
    private final BPlusTree<ValueEntry> store = new BPlusTree<>();
    private final Map<String, Long> keyVersions = new HashMap<>();
    private final PriorityQueue<Expiry> expirations = new PriorityQueue<>(
            java.util.Comparator.comparingLong(Expiry::timestamp));
    private record Expiry(String key, long timestamp) { }
    private record Usage(long bytes, int keys) { }
    private long commitVersion;
    private long liveBytes;
    private int liveKeys;
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
            rocksDb.forEachEntry((key, value) -> {
                commitVersion++;
                keyVersions.put(key, commitVersion);
                if (value.isExpired()) {
                    try { rocksDb.delete(key); }
                    catch (IOException e) { throw new UncheckedIOException("Could not remove expired RocksDB record", e); }
                } else {
                    accountRecoveredPut(key, value);
                    if (value.hasExpiry()) expirations.add(new Expiry(key, value.getExpiryTimestampMillis()));
                }
            });
            return;
        }
        for (WriteAheadLog.Record record : wal.replay()) {
            commitVersion++;
            keyVersions.put(record.getKey(), commitVersion);
            if (record.getOperation() == WriteAheadLog.Operation.DELETE) {
                ValueEntry prior = store.get(record.getKey());
                if (prior != null) accountDelete(record.getKey(), prior);
                store.remove(record.getKey());
            } else {
                long expiry = record.getExpiryEpochMillis();
                if (expiry >= 0 && expiry <= now) {
                    ValueEntry previous = store.get(record.getKey());
                    if (previous != null) accountDelete(record.getKey(), previous);
                    store.remove(record.getKey());
                } else if (expiry < 0) {
                    ValueEntry value = new ValueEntry(record.getValue());
                    accountRecoveredPut(record.getKey(), value); store.put(record.getKey(), value);
                } else {
                    ValueEntry value = new ValueEntry(record.getValue(), expiry);
                    accountRecoveredPut(record.getKey(), value); store.put(record.getKey(), value);
                    expirations.add(new Expiry(record.getKey(), expiry));
                }
            }
        }
    }

    /** Stores a key without expiration. */
    public synchronized void set(String key, String value) {
        Usage usage = ensureCapacity(Map.of(key, value));
        appendPut(key, value, -1L);
        liveBytes = usage.bytes(); liveKeys = usage.keys();
        if (rocksDb == null) store.put(key, new ValueEntry(value));
        recordMutation(key);
    }

    /** Durably writes a batch with one WAL force, then publishes all values. */
    public synchronized void setBatch(Map<String, String> values) {
        if (values == null || values.isEmpty()) return;
        Usage usage = ensureCapacity(values);
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
        liveBytes = usage.bytes(); liveKeys = usage.keys();
        if (rocksDb == null) for (Map.Entry<String, String> entry : values.entrySet())
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
        Usage usage = ensureCapacity(Map.of(key, value));
        appendPut(key, value, expiryTimestamp);
        liveBytes = usage.bytes(); liveKeys = usage.keys();
        if (rocksDb == null) store.put(key, new ValueEntry(value, expiryTimestamp));
        expirations.add(new Expiry(key, expiryTimestamp));
        recordMutation(key);
    }

    /** Stores a key with an absolute Unix expiry timestamp in milliseconds. */
    public synchronized void setWithAbsoluteExpiry(
            String key, String value, long expiryTimestampMillis) {
        if (expiryTimestampMillis <= System.currentTimeMillis()) {
            del(key);
            return;
        }
        Usage usage = ensureCapacity(Map.of(key, value));
        appendPut(key, value, expiryTimestampMillis);
        liveBytes = usage.bytes(); liveKeys = usage.keys();
        if (rocksDb == null) store.put(key, new ValueEntry(value, expiryTimestampMillis));
        expirations.add(new Expiry(key, expiryTimestampMillis));
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

    public synchronized String get(String key) {
        ValueEntry entry = readCurrent(key);
        if (entry == null) {
            return null;
        }
        if (entry.isExpired()) {
            if (rocksDb == null) store.removeIfSame(key, entry);
            accountDelete(key, entry);
            // The durable value retains its absolute expiry; avoid deleting a concurrent replacement.
            return null;
        }
        return entry.getValue();
    }

    public synchronized ValueEntry getEntry(String key) {
        ValueEntry entry = readCurrent(key);
        if (entry == null) {
            return null;
        }
        if (entry.isExpired()) {
            if (rocksDb == null) store.removeIfSame(key, entry);
            accountDelete(key, entry);
            // The durable value retains its absolute expiry; avoid deleting a concurrent replacement.
            return null;
        }
        return entry;
    }

    private ValueEntry readCurrent(String key) {
        if (rocksDb == null) return store.get(key);
        try { return rocksDb.getEntry(key); }
        catch (IOException e) { throw new UncheckedIOException("Could not read RocksDB key", e); }
    }

    public synchronized boolean exists(String key) {
        return get(key) != null;
    }

    public synchronized int del(String... keys) {
        int count = 0;
        for (String key : keys) {
            // Record the delete even when the key is absent, so an older PUT
            // cannot be resurrected during replay.
            if (rocksDb != null) {
                ValueEntry previous = readCurrent(key);
                appendDelete(key);
                if (previous != null) accountDelete(key, previous);
                if (previous != null && !previous.isExpired()) count++;
            } else {
                appendDelete(key);
                ValueEntry removed = store.remove(key);
                if (removed != null) accountDelete(key, removed);
                if (removed != null && !removed.isExpired()) count++;
            }
            recordMutation(key);
        }
        return count;
    }

    /** Deletes the supplied keys as one durable storage transaction. */
    public synchronized int deleteBatchAtomic(String... keys) {
        Objects.requireNonNull(keys, "keys");
        Map<String, String> changes = new LinkedHashMap<>();
        int count = 0;
        for (String key : new java.util.LinkedHashSet<>(java.util.Arrays.asList(keys))) {
            Objects.requireNonNull(key, "key");
            ValueEntry current = readCurrent(key);
            if (current != null && !current.isExpired()) count++;
            changes.put(key, null);
        }
        commitTransaction(commitVersion, changes);
        return count;
    }

    /** Applies puts and deletes as one checksummed WAL record or RocksDB batch. */
    public synchronized void applyAtomicChanges(Map<String, String> changes) {
        Objects.requireNonNull(changes, "changes");
        commitTransaction(commitVersion, new LinkedHashMap<>(changes));
    }

    /** Starts a repeatable-read snapshot transaction. */
    public synchronized MVCCTransaction beginTransaction() {
        Map<String, String> snapshot = new HashMap<>();
        for (Map.Entry<String, ValueEntry> entry : entriesSnapshot().entrySet()) {
            ValueEntry value = entry.getValue();
            if (value.isExpired()) {
                if (rocksDb == null) store.remove(entry.getKey());
                deleteExpiredFromRocksDb(entry.getKey());
                accountDelete(entry.getKey(), value);
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
        Usage usage = ensureCapacity(changes);
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
        liveBytes = usage.bytes(); liveKeys = usage.keys();

        long transactionVersion = ++commitVersion;
        for (Map.Entry<String, String> change : changes.entrySet()) {
            keyVersions.put(change.getKey(), transactionVersion);
            if (rocksDb == null) {
                if (change.getValue() == null) store.remove(change.getKey());
                else store.put(change.getKey(), new ValueEntry(change.getValue()));
            }
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

        for (Map.Entry<String, ValueEntry> e : entriesSnapshot().entrySet()) {
            ValueEntry entry = e.getValue();
            if (entry.hasExpiry() && entry.getExpiryTimestampMillis() <= now) {
                if (rocksDb == null) store.remove(e.getKey());
                deleteExpiredFromRocksDb(e.getKey());
                accountDelete(e.getKey(), entry);
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
        while (!expirations.isEmpty() && expirations.peek().timestamp() <= now) {
            Expiry expiry = expirations.poll();
            ValueEntry value = readCurrent(expiry.key());
            // A newer SET leaves an obsolete heap record; expiry equality makes it harmless.
            if (value != null && value.getExpiryTimestampMillis() == expiry.timestamp() && value.isExpired()) {
                if (rocksDb == null) store.removeIfSame(expiry.key(), value);
                else deleteExpiredFromRocksDb(expiry.key());
                accountDelete(expiry.key(), value);
            }
        }
    }

    public synchronized int size() {
        cleanupExpiredKeys();
        if (rocksDb == null) return store.size();
        try { return rocksDb.countLiveEntries(); }
        catch (IOException e) { throw new UncheckedIOException("Could not count RocksDB keys", e); }
    }

    private Map<String, ValueEntry> entriesSnapshot() {
        if (rocksDb == null) return store.snapshot();
        try { return rocksDb.entriesSnapshot(); }
        catch (IOException e) { throw new UncheckedIOException("Could not scan RocksDB", e); }
    }

    /** Expiry-preserving point-in-time data view for Raft state snapshots. */
    public synchronized Map<String, ValueEntry> snapshotEntries() {
        cleanupExpiredKeys();
        return java.util.Collections.unmodifiableMap(new LinkedHashMap<>(entriesSnapshot()));
    }

    /** Replaces the durable data set; callers must first persist their recovery snapshot. */
    public synchronized void replaceSnapshot(Map<String, ValueEntry> entries) throws IOException {
        Objects.requireNonNull(entries, "entries");
        Map<String, String> values = new LinkedHashMap<>();
        long snapshotBytes = 0;
        entries.forEach((key, value) -> {
            if (key == null || value == null) throw new IllegalArgumentException("Null snapshot row");
            if (!value.isExpired()) values.put(key, value.getValue());
        });
        Map<String, ValueEntry> live = new LinkedHashMap<>();
        for (Map.Entry<String, ValueEntry> row : entries.entrySet()) {
            String key = row.getKey(); ValueEntry value = row.getValue();
            int keyBytes = key.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            int valueBytes = value.getValue().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            if (keyBytes == 0 || keyBytes > MAX_KEY_BYTES || valueBytes > MAX_VALUE_BYTES)
                throw new IllegalArgumentException("Snapshot key/value exceeds configured limits");
            if (!value.isExpired()) { live.put(key, value); snapshotBytes += keyBytes + (long) valueBytes; }
        }
        if (live.size() > MAX_LIVE_KEYS || snapshotBytes > MAX_LIVE_BYTES)
            throw new IllegalStateException("Snapshot exceeds configured live storage limits");
        if (wal != null) wal.replaceWithSnapshot(live);
        else rocksDb.replaceAll(live);
        store.clear(); keyVersions.clear(); expirations.clear(); liveBytes = 0; liveKeys = 0;
        commitVersion++;
        for (Map.Entry<String, ValueEntry> entry : live.entrySet()) {
            if (rocksDb == null) store.put(entry.getKey(), entry.getValue());
            keyVersions.put(entry.getKey(), commitVersion);
            accountRecoveredPut(entry.getKey(), entry.getValue());
            if (entry.getValue().hasExpiry()) expirations.add(new Expiry(entry.getKey(), entry.getValue().getExpiryTimestampMillis()));
        }
    }

    private Usage ensureCapacity(Map<String, String> changes) {
        long nextBytes = liveBytes; int nextKeys = liveKeys;
        for (Map.Entry<String, String> change : changes.entrySet()) {
            String key = Objects.requireNonNull(change.getKey(), "key");
            String value = change.getValue();
            int keyBytes = key.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            if (keyBytes == 0 || keyBytes > MAX_KEY_BYTES) throw new IllegalArgumentException("Key exceeds KV_MAX_KEY_BYTES");
            ValueEntry previous = readCurrent(key);
            if (previous != null) { nextBytes -= entryBytes(key, previous); }
            else if (value != null) nextKeys++;
            if (value == null) { if (previous != null) nextKeys--; }
            else {
                int valueBytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
                if (valueBytes > MAX_VALUE_BYTES) throw new IllegalArgumentException("Value exceeds KV_MAX_VALUE_BYTES");
                nextBytes += keyBytes + (long) valueBytes;
            }
        }
        if (nextKeys > MAX_LIVE_KEYS) throw new IllegalStateException("Live key limit reached (KV_MAX_LIVE_KEYS)");
        if (nextBytes > MAX_LIVE_BYTES) throw new IllegalStateException("Live storage byte limit reached (KV_MAX_LIVE_BYTES)");
        return new Usage(nextBytes, nextKeys);
    }

    private void accountRecoveredPut(String key, ValueEntry value) {
        int keyBytes = key.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        int valueBytes = value.getValue().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        if (keyBytes == 0 || keyBytes > MAX_KEY_BYTES || valueBytes > MAX_VALUE_BYTES)
            throw new IllegalStateException("Stored key/value exceeds configured KV memory bounds");
        ValueEntry previous = rocksDb == null ? store.get(key) : null;
        if (previous != null) accountDelete(key, previous);
        long bytes = entryBytes(key, value);
        if (previous == null) liveKeys++;
        liveBytes += bytes;
        checkRecoveredLimits();
    }
    private void accountDelete(String key, ValueEntry value) {
        liveBytes = Math.max(0, liveBytes - entryBytes(key, value));
        liveKeys = Math.max(0, liveKeys - 1);
    }
    private static long entryBytes(String key, ValueEntry value) {
        return key.getBytes(java.nio.charset.StandardCharsets.UTF_8).length
                + value.getValue().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
    }
    private void checkRecoveredLimits() {
        if (liveKeys > MAX_LIVE_KEYS || liveBytes > MAX_LIVE_BYTES)
            throw new IllegalStateException("Stored data exceeds configured KV memory bounds");
    }
    private static int positiveIntSetting(String name, int fallback) {
        String raw = System.getenv(name); if (raw == null || raw.isBlank()) return fallback;
        try { int value = Integer.parseInt(raw); if (value <= 0) throw new IllegalArgumentException(name + " must be positive"); return value; }
        catch (NumberFormatException e) { throw new IllegalArgumentException(name + " must be an integer", e); }
    }
    private static long positiveLongSetting(String name, long fallback) {
        String raw = System.getenv(name); if (raw == null || raw.isBlank()) return fallback;
        try { long value = Long.parseLong(raw); if (value <= 0) throw new IllegalArgumentException(name + " must be positive"); return value; }
        catch (NumberFormatException e) { throw new IllegalArgumentException(name + " must be an integer", e); }
    }

    public synchronized void clear() {
        del(entriesSnapshot().keySet().toArray(new String[0]));
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
