package com.codecrafters.redis.client;

import com.codecrafters.redis.protocol.RespType;
import com.codecrafters.redis.protocol.RespValue;
import java.io.IOException;
import javax.net.ssl.SSLContext;
import java.nio.charset.StandardCharsets;

/** Synchronous key/value operations over a reusable RESP session. */
public final class KVClientImpl implements KVClient {
    private final Session session;
    public KVClientImpl(String host, int port) { this(new Session(host, port)); }
    public KVClientImpl(String host, int port, int connectTimeoutMillis, int readTimeoutMillis) {
        this(new Session(host, port, connectTimeoutMillis, readTimeoutMillis));
    }
    public KVClientImpl(String host, int port, boolean tls, String password) {
        this(new Session(host, port, tls, password));
    }
    public KVClientImpl(String host, int port, boolean tls, SSLContext sslContext,
                        String username, String password) {
        this(new Session(host, port, tls, sslContext, username, password));
    }
    public KVClientImpl(Session session) {
        if (session == null) throw new IllegalArgumentException("session must not be null");
        this.session = session;
    }
    @Override public void connect() throws IOException { session.connect(); }
    @Override public String get(String key) throws IOException {
        byte[] value = getBytes(key);
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }
    @Override public byte[] getBytes(String key) throws IOException {
        RespValue response = session.execute(bytes("GET"), requireKey(key).getBytes(StandardCharsets.UTF_8));
        if (response.isNull()) return null;
        if (response.getType() != RespType.BULK_STRING) throw unexpected("GET", response);
        return response.getBytesValue();
    }
    @Override public void set(String key, String value) throws IOException {
        setBytes(key, requireValue(value).getBytes(StandardCharsets.UTF_8));
    }
    @Override public void setBytes(String key, byte[] value) throws IOException {
        if (value == null) throw new IllegalArgumentException("value must not be null");
        RespValue response = session.execute(bytes("SET"), requireKey(key).getBytes(StandardCharsets.UTF_8), value.clone());
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
    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
    private static IOException unexpected(String command, RespValue response) {
        return new IOException("Unexpected " + command + " response: " + response);
    }
}
