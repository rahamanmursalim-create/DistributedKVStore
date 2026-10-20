package com.codecrafters.redis.storage;

import java.util.LinkedHashMap;
import java.util.Map;

/** Snapshot-isolated transaction over a RedisStore. */
public final class MVCCTransaction implements AutoCloseable {
    private final RedisStore store;
    private final long snapshotVersion;
    private final Map<String, String> snapshot;
    private final Map<String, String> changes = new LinkedHashMap<>();
    private boolean finished;

    MVCCTransaction(RedisStore store, long snapshotVersion, Map<String, String> snapshot) {
        this.store = store;
        this.snapshotVersion = snapshotVersion;
        this.snapshot = snapshot;
    }

    public synchronized String get(String key) {
        ensureActive();
        if (key == null) throw new IllegalArgumentException("Key cannot be null");
        return changes.containsKey(key) ? changes.get(key) : snapshot.get(key);
    }

    public synchronized void put(String key, String value) {
        ensureActive();
        if (key == null || value == null) throw new IllegalArgumentException("Key and value cannot be null");
        changes.put(key, value);
    }

    /** Stages a delete and returns whether the key existed in this transaction's view. */
    public synchronized boolean delete(String key) {
        ensureActive();
        if (key == null) throw new IllegalArgumentException("Key cannot be null");
        boolean existed = get(key) != null;
        changes.put(key, null);
        return existed;
    }

    /** Validates write conflicts, durably commits all staged changes, and closes the transaction. */
    public synchronized void commit() {
        ensureActive();
        try {
            store.commitTransaction(snapshotVersion, changes);
        } finally {
            finished = true;
            changes.clear();
        }
    }

    public synchronized void rollback() {
        ensureActive();
        finished = true;
        changes.clear();
    }

    public long getSnapshotVersion() { return snapshotVersion; }

    private void ensureActive() {
        if (finished) throw new IllegalStateException("Transaction is already finished");
    }

    @Override
    public void close() {
        synchronized (this) {
            if (!finished) {
                finished = true;
                changes.clear();
            }
        }
    }
}
