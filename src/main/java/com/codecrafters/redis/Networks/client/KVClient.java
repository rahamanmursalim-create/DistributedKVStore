package com.codecrafters.redis.client;

import java.io.IOException;

/** A synchronous client for the project's Redis-compatible TCP/RESP API. */
public interface KVClient extends AutoCloseable {
    void connect() throws IOException;
    String get(String key) throws IOException;
    void set(String key, String value) throws IOException;
    default byte[] getBytes(String key) throws IOException {
        String value = get(key);
        return value == null ? null : value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
    default void setBytes(String key, byte[] value) throws IOException {
        if (value == null) throw new IllegalArgumentException("value must not be null");
        set(key, new String(value, java.nio.charset.StandardCharsets.UTF_8));
    }
    long delete(String... keys) throws IOException;
    @Override void close() throws IOException;
}
