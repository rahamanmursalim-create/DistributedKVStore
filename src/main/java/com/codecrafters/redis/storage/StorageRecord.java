package com.codecrafters.redis.storage;

/**
 * Immutable value and optional absolute expiration timestamp.
 * A negative expiry timestamp means no expiration.
 */
public final class StorageRecord {
    private final String value;
    private final long expiryEpochMillis;

    public StorageRecord(String value, long expiryEpochMillis) {
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }
        if (expiryEpochMillis < 0 && expiryEpochMillis != -1L) {
            throw new IllegalArgumentException(
                    "Expiry must be -1 or a non-negative timestamp");
        }

        this.value = value;
        this.expiryEpochMillis = expiryEpochMillis;
    }

    public String getValue() {
        return value;
    }

    public long getExpiryEpochMillis() {
        return expiryEpochMillis;
    }

    public boolean hasExpiry() {
        return expiryEpochMillis >= 0;
    }

    public boolean isExpired() {
        return hasExpiry()
                && System.currentTimeMillis() >= expiryEpochMillis;
    }
}