package com.codecrafters.redis.client;

import com.codecrafters.redis.protocol.RespType;
import com.codecrafters.redis.protocol.RespValue;
import java.io.IOException;

/** Synchronous key/value operations over a reusable RESP session. */
public final class KVClientImpl implements KVClient {
    private final Session session;
    public KVClientImpl(String host, int port) { this(new Session(host, port)); }
    public KVClientImpl(String host, int port, int connectTimeoutMillis, int readTimeoutMillis) {
        this(new Session(host, port, connectTimeoutMillis, readTimeoutMillis));
    }
    public KVClientImpl(Session session) {
        if (session == null) throw new IllegalArgumentException("session must not be null");
        this.session = session;
    }
    @Override public void connect() throws IOException { session.connect(); }
    @Override public String get(String key) throws IOException {
        RespValue response = session.execute("GET", requireKey(key));
        if (response.isNull()) return null;
        if (response.getType() != RespType.BULK_STRING) throw unexpected("GET", response);
        return response.getStringValue();
    }
    @Override public void set(String key, String value) throws IOException {
        RespValue response = session.execute("SET", requireKey(key), requireValue(value));
        if (response.getType() != RespType.SIMPLE_STRING || !"OK".equals(response.getStringValue())) throw unexpected("SET", response);
    }
    @Override public long delete(String... keys) throws IOException {
        if (keys == null || keys.length == 0) throw new IllegalArgumentException("at least one key is required");
        String[] command = new String[keys.length + 1];
        command[0] = "DEL";
        for (int i = 0; i < keys.length; i++) command[i + 1] = requireKey(keys[i]);
        RespValue response = session.execute(command);
        if (response.getType() != RespType.INTEGER) throw unexpected("DEL", response);
        return response.getIntegerValue();
    }
    @Override public void close() throws IOException { session.close(); }
    private static String requireKey(String key) {
        if (key == null) throw new IllegalArgumentException("key must not be null");
        return key;
    }
    private static String requireValue(String value) {
        if (value == null) throw new IllegalArgumentException("value must not be null");
        return value;
    }
    private static IOException unexpected(String command, RespValue response) {
        return new IOException("Unexpected " + command + " response: " + response);
    }
}
