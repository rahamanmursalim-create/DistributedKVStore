package com.codecrafters.redis.client;

import java.io.IOException;

/** A synchronous client for the project's Redis-compatible TCP/RESP API. */
public interface KVClient extends AutoCloseable {
    void connect() throws IOException;
    String get(String key) throws IOException;
    void set(String key, String value) throws IOException;
    long delete(String... keys) throws IOException;
    @Override void close() throws IOException;
}
