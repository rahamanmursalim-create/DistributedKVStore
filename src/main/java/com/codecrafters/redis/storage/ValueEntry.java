package com.codecrafters.redis.storage;

/**
 * An entry stored in the RedisStore containing the value and optional expiration timestamp.
 */
public final class ValueEntry {
    private final String value;
    private final long expiryTimestampMillis; // <= 0 indicates no expiration

    public ValueEntry(String value) {
        this(value, -1);
    }

    public ValueEntry(String value, long expiryTimestampMillis) {
        this.value = value;
        this.expiryTimestampMillis = expiryTimestampMillis;
    }

    public String getValue() {
        return value;
    }

    public long getExpiryTimestampMillis() {
        return expiryTimestampMillis;
    }

    public boolean hasExpiry() {
        return expiryTimestampMillis > 0;
    }

    public boolean isExpired() {
        return expiryTimestampMillis > 0 && System.currentTimeMillis() >= expiryTimestampMillis;
    }

    public long getTtlMillis() {
        if (expiryTimestampMillis <= 0) {
            return -1; // No TTL
        }
        long remaining = expiryTimestampMillis - System.currentTimeMillis();
        return Math.max(0, remaining);
    }
}
