package com.codecrafters.redis.storage;

/** Raised when a transaction writes a key changed after its snapshot began. */
public final class TransactionConflictException extends RuntimeException {
    public TransactionConflictException(String key) {
        super("Snapshot transaction conflict on key: " + key);
    }
}
