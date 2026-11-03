package com.codecrafters.redis.client;

import com.codecrafters.redis.protocol.RespReader;
import com.codecrafters.redis.protocol.RespType;
import com.codecrafters.redis.protocol.RespValue;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Owns one persistent TCP connection and serializes request/response exchanges. */
public final class Session implements AutoCloseable {
    private final String host;
    private final int port;
    private final int connectTimeoutMillis;
    private final int readTimeoutMillis;
    private final boolean tls;
    private final SSLContext sslContext;
    private final String username;
    private final String password;
    private Socket socket;
    private BufferedInputStream input;
    private BufferedOutputStream output;
    private RespReader reader;

    public Session(String host, int port) { this(host, port, 3_000, 0); }

    public Session(String host, int port, int connectTimeoutMillis, int readTimeoutMillis) {
        this(host, port, connectTimeoutMillis, readTimeoutMillis, false, null, null, null);
    }

    public Session(String host, int port, boolean tls, String password) {
        this(host, port, 3_000, 0, tls, null, null, password);
    }

    public Session(String host, int port, boolean tls, SSLContext sslContext,
                   String username, String password) {
        this(host, port, 3_000, 0, tls, sslContext, username, password);
    }

    public Session(String host, int port, int connectTimeoutMillis, int readTimeoutMillis,
                   boolean tls, SSLContext sslContext, String username, String password) {
        if (host == null || host.isBlank()) throw new IllegalArgumentException("host must not be blank");
        if (port < 1 || port > 65_535) throw new IllegalArgumentException("port out of range");
        if (connectTimeoutMillis < 0 || readTimeoutMillis < 0) throw new IllegalArgumentException("timeouts must be non-negative");
        this.host = host;
        this.port = port;
        this.connectTimeoutMillis = connectTimeoutMillis;
        this.readTimeoutMillis = readTimeoutMillis;
        this.tls = tls;
        this.sslContext = sslContext;
        this.username = username;
        this.password = password;
        if (username != null && (password == null || password.isEmpty()))
            throw new IllegalArgumentException("password is required when username is set");
    }

    public synchronized void connect() throws IOException {
        if (socket != null && socket.isConnected() && !socket.isClosed()) return;
        close();
        Socket candidate = tls
                ? (sslContext == null ? (SSLSocketFactory) SSLSocketFactory.getDefault() : sslContext.getSocketFactory()).createSocket()
                : new Socket();
        try {
            candidate.connect(new InetSocketAddress(host, port), connectTimeoutMillis);
            candidate.setSoTimeout(readTimeoutMillis);
            if (tls) {
                SSLSocket secure = (SSLSocket) candidate;
                SSLParameters parameters = secure.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                secure.setSSLParameters(parameters);
                secure.startHandshake();
            }
            socket = candidate;
            input = new BufferedInputStream(candidate.getInputStream());
            output = new BufferedOutputStream(candidate.getOutputStream());
            reader = new RespReader(input);
            if (password != null) {
                byte[][] auth = username == null
                        ? new byte[][] {bytes("AUTH"), bytes(password)}
                        : new byte[][] {bytes("AUTH"), bytes(username), bytes(password)};
                RespValue response = exchange(auth);
                if (response.getType() != RespType.SIMPLE_STRING || !"OK".equals(response.getStringValue()))
                    throw new IOException("Redis AUTH failed");
            }
        } catch (IOException e) {
            try { candidate.close(); } catch (IOException closeError) { e.addSuppressed(closeError); }
            throw e;
        }
    }

    public synchronized RespValue execute(String... command) throws IOException {
        if (command == null || command.length == 0) throw new IllegalArgumentException("command must not be empty");
        byte[][] arguments = new byte[command.length][];
        for (int i = 0; i < command.length; i++) {
            String argument = command[i];
            if (argument == null) throw new IllegalArgumentException("command arguments must not be null");
            arguments[i] = argument.getBytes(StandardCharsets.UTF_8);
        }
        return execute(arguments);
    }

    public synchronized RespValue execute(byte[]... command) throws IOException {
        if (command == null || command.length == 0) throw new IllegalArgumentException("command must not be empty");
        for (byte[] argument : command) if (argument == null)
            throw new IllegalArgumentException("command arguments must not be null");
        connect();
        try { return exchange(command); }
        catch (SocketTimeoutException e) {
            close();
            throw new IOException("Timed out waiting for server response", e);
        } catch (IOException e) {
            close();
            throw e;
        }
    }

    private RespValue exchange(byte[][] command) throws IOException {
        List<RespValue> arguments = new ArrayList<>(command.length);
        for (byte[] argument : command) arguments.add(RespValue.bulkString(argument));
        output.write(RespValue.array(arguments).encode());
        output.flush();
        RespValue response = reader.read();
        if (response == null) throw new IOException("Server closed the connection without a response");
        if (response.getType() == RespType.ERROR) throw new IOException("Server error: " + response.getStringValue());
        return response;
    }

    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }

    @Override public synchronized void close() throws IOException {
        Socket current = socket;
        socket = null;
        input = null;
        output = null;
        reader = null;
        if (current != null) current.close();
    }
}
