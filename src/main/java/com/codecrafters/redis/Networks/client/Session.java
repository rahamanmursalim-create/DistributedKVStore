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
import java.util.ArrayList;
import java.util.List;

/** Owns one persistent TCP connection and serializes request/response exchanges. */
public final class Session implements AutoCloseable {
    private final String host;
    private final int port;
    private final int connectTimeoutMillis;
    private final int readTimeoutMillis;
    private Socket socket;
    private BufferedInputStream input;
    private BufferedOutputStream output;
    private RespReader reader;

    public Session(String host, int port) { this(host, port, 3_000, 0); }

    public Session(String host, int port, int connectTimeoutMillis, int readTimeoutMillis) {
        if (host == null || host.isBlank()) throw new IllegalArgumentException("host must not be blank");
        if (port < 1 || port > 65_535) throw new IllegalArgumentException("port out of range");
        if (connectTimeoutMillis < 0 || readTimeoutMillis < 0) throw new IllegalArgumentException("timeouts must be non-negative");
        this.host = host;
        this.port = port;
        this.connectTimeoutMillis = connectTimeoutMillis;
        this.readTimeoutMillis = readTimeoutMillis;
    }

    public synchronized void connect() throws IOException {
        if (socket != null && socket.isConnected() && !socket.isClosed()) return;
        close();
        Socket candidate = new Socket();
        try {
            candidate.connect(new InetSocketAddress(host, port), connectTimeoutMillis);
            candidate.setSoTimeout(readTimeoutMillis);
            socket = candidate;
            input = new BufferedInputStream(candidate.getInputStream());
            output = new BufferedOutputStream(candidate.getOutputStream());
            reader = new RespReader(input);
        } catch (IOException e) {
            try { candidate.close(); } catch (IOException closeError) { e.addSuppressed(closeError); }
            throw e;
        }
    }

    public synchronized RespValue execute(String... command) throws IOException {
        if (command == null || command.length == 0) throw new IllegalArgumentException("command must not be empty");
        List<RespValue> arguments = new ArrayList<>(command.length);
        for (String argument : command) {
            if (argument == null) throw new IllegalArgumentException("command arguments must not be null");
            arguments.add(RespValue.bulkString(argument));
        }
        connect();
        try {
            output.write(RespValue.array(arguments).encode());
            output.flush();
            RespValue response = reader.read();
            if (response == null) throw new IOException("Server closed the connection without a response");
            if (response.getType() == RespType.ERROR) throw new IOException("Server error: " + response.getStringValue());
            return response;
        } catch (SocketTimeoutException e) {
            close();
            throw new IOException("Timed out waiting for server response", e);
        } catch (IOException e) {
            close();
            throw e;
        }
    }

    @Override public synchronized void close() throws IOException {
        Socket current = socket;
        socket = null;
        input = null;
        output = null;
        reader = null;
        if (current != null) current.close();
    }
}
