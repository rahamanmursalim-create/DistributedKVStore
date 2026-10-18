package com.codecrafters.redis.distributed;

import com.codecrafters.redis.storage.StorageEngine;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Receives internal replication and query requests over TCP.
 *
 * Protocol:
 * PUT:    byte 1, key, value, expiry timestamp -> response byte 1
 * DELETE: byte 2, key -> response byte 1
 * GET:    byte 3, key -> response byte 1 + value string, or response byte 0 (not found/null)
 *
 * Strings are encoded as a 32-bit length followed by UTF-8 bytes.
 */
public final class ReplicationServer implements AutoCloseable {

    private static final byte PUT = 1;
    private static final byte DELETE = 2;
    private static final byte GET = 3;
    private static final int MAX_STRING_BYTES = 1_048_576;

    private final int port;
    private final StorageEngine storage;
    private final ServerSocket serverSocket;
    private final ExecutorService workers;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public ReplicationServer(int port, StorageEngine storage)
            throws IOException {

        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Invalid port");
        }

        if (storage == null) {
            throw new IllegalArgumentException("Storage cannot be null");
        }

        this.port = port;
        this.storage = storage;
        this.serverSocket = new ServerSocket();

        // Keep the internal replication endpoint local-only for now.
        this.serverSocket.bind(new InetSocketAddress(
                InetAddress.getLoopbackAddress(), port));

        this.workers = Executors.newFixedThreadPool(4);
    }

    public void start() throws IOException {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException(
                    "Replication server is already running");
        }

        try {
            while (running.get()) {
                try {
                    Socket socket = serverSocket.accept();
                    workers.execute(() -> handleClient(socket));
                } catch (SocketException e) {
                    if (running.get()) {
                        throw e;
                    }
                }
            }
        } finally {
            running.set(false);
        }
    }

    private void handleClient(Socket socket) {
        try (socket;
             DataInputStream in = new DataInputStream(
                     new BufferedInputStream(socket.getInputStream()));
             DataOutputStream out = new DataOutputStream(
                     new BufferedOutputStream(socket.getOutputStream()))) {

            socket.setSoTimeout(5000);

            byte operation = in.readByte();

            if (operation == PUT) {
                String key = readString(in);
                String value = readString(in);
                long expiryEpochMillis = in.readLong();

                storage.put(key, value, expiryEpochMillis);

                out.writeByte(1);
                out.flush();

            } else if (operation == DELETE) {
                String key = readString(in);
                storage.delete(key);

                out.writeByte(1);
                out.flush();

            } else if (operation == GET) {
                String key = readString(in);
                String value = storage.get(key);

                if (value == null) {
                    out.writeByte(0); // Absent, deleted, or expired
                } else {
                    out.writeByte(1); // Present
                    writeString(out, value);
                }
                out.flush();

            } else {
                throw new IOException("Unknown replication operation: " + operation);
            }

        } catch (EOFException e) {
            System.err.println("Incomplete replication request");
        } catch (IOException | RuntimeException e) {
            System.err.println(
                    "Replication request failed: " + e.getMessage());
        }
    }

    private static String readString(DataInputStream in)
            throws IOException {

        int length = in.readInt();

        if (length < 0 || length > MAX_STRING_BYTES) {
            throw new IOException("Invalid string length: " + length);
        }

        byte[] bytes = new byte[length];
        in.readFully(bytes);

        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static void writeString(
            DataOutputStream out,
            String value) throws IOException {

        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);

        if (bytes.length > MAX_STRING_BYTES) {
            throw new IOException("String exceeds maximum size");
        }

        out.writeInt(bytes.length);
        out.write(bytes);
    }

    public int getPort() {
        return port;
    }

    @Override
    public void close() throws IOException {
        running.set(false);
        serverSocket.close();
        workers.shutdownNow();
    }
}