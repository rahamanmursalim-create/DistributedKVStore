package com.codecrafters.redis.storage;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Thread-safe key-value store with TTL and WAL-backed restart recovery.
 *
 * The WAL is written and synced before an in-memory mutation is published.
 * Default WAL location: ./data/wal.log (relative to the process working directory).
 */
public class RedisStore implements AutoCloseable {
    private final ConcurrentHashMap<String, ValueEntry> store = new ConcurrentHashMap<>();
    private final ScheduledExecutorService cleanupExecutor;
    private final WriteAheadLog wal;

    public RedisStore() {
        this(true);
    }

    public RedisStore(boolean enableBackgroundCleanup) {
        this(Path.of("data", "wal.log"), enableBackgroundCleanup);
    }

    public RedisStore(Path walPath, boolean enableBackgroundCleanup) {
        try {
            this.wal = new WriteAheadLog(walPath);
            recoverFromWal();
        } catch (IOException e) {
            throw new UncheckedIOException("Could not open/recover WAL at " + walPath, e);
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

    private void recoverFromWal() throws IOException {
        long now = System.currentTimeMillis();
        for (WriteAheadLog.Record record : wal.replay()) {
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
    }

    private void appendPut(String key, String value, long expiryTimestamp) {
        try {
            wal.appendPut(key, value, expiryTimestamp);
        } catch (IOException e) {
            throw new UncheckedIOException("Could not persist PUT for key " + key, e);
        }
    }

    public synchronized String get(String key) {
        ValueEntry entry = store.get(key);
        if (entry == null) {
            return null;
        }
        if (entry.isExpired()) {
            store.remove(key, entry);
            return null;
        }
        return entry.getValue();
    }

    public synchronized ValueEntry getEntry(String key) {
        ValueEntry entry = store.get(key);
        if (entry == null) {
            return null;
        }
        if (entry.isExpired()) {
            store.remove(key, entry);
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
            try {
                wal.appendDelete(key);
            } catch (IOException e) {
                throw new UncheckedIOException("Could not persist DELETE for key " + key, e);
            }
            ValueEntry removed = store.remove(key);
            if (removed != null && !removed.isExpired()) {
                count++;
            }
        }
        return count;
    }

    public synchronized List<String> keys(String pattern) {
        Pattern regex = globToRegex(pattern);
        List<String> matchedKeys = new ArrayList<>();
        long now = System.currentTimeMillis();

        for (Map.Entry<String, ValueEntry> e : store.entrySet()) {
            ValueEntry entry = e.getValue();
            if (entry.hasExpiry() && entry.getExpiryTimestampMillis() <= now) {
                store.remove(e.getKey(), entry);
                continue;
            }
            if (regex.matcher(e.getKey()).matches()) {
                matchedKeys.add(e.getKey());
            }
        }
        return matchedKeys;
    }

    public void cleanupExpiredKeys() {
        long now = System.currentTimeMillis();
        for (Map.Entry<String, ValueEntry> entry : store.entrySet()) {
            ValueEntry value = entry.getValue();
            if (value.hasExpiry() && value.getExpiryTimestampMillis() <= now) {
                store.remove(entry.getKey(), value);
            }
        }
    }

    public int size() {
        cleanupExpiredKeys();
        return store.size();
    }

    public synchronized void clear() {
        del(store.keySet().toArray(new String[0]));
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
        try {
            wal.close();
        } catch (IOException e) {
            throw new UncheckedIOException("Could not close WAL", e);
        }
    }
}
