package com.codecrafters.redis.distributed;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * Sends replication and query operations to a remote ReplicationServer over TCP.
 */
public final class ReplicationClient {

    private static final byte PUT = 1;
    private static final byte DELETE = 2;
    private static final byte GET = 3;
    private static final int MAX_STRING_BYTES = 1_048_576;
    private static final int CONNECT_TIMEOUT_MS = 3000;
    private static final int READ_TIMEOUT_MS = 5000;

    private static final int DEFAULT_MAX_RETRIES = 2;
    private static final int RETRY_BACKOFF_MS = 50;

    private final String host;
    private final int port;
    private final int maxRetries;

    public ReplicationClient(String host, int port) {
        this(host, port, DEFAULT_MAX_RETRIES);
    }

    public ReplicationClient(String host, int port, int maxRetries) {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("Host cannot be empty");
        }

        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Invalid port");
        }

        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be non-negative");
        }

        this.host = host;
        this.port = port;
        this.maxRetries = maxRetries;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void replicatePut(
            String key,
            String value,
            long expiryEpochMillis) throws IOException {

        if (key == null || value == null) {
            throw new IllegalArgumentException(
                    "Key and value cannot be null");
        }

        if (expiryEpochMillis < -1L) {
            throw new IllegalArgumentException(
                    "Invalid expiry timestamp");
        }

        send(out -> {
            out.writeByte(PUT);
            writeString(out, key);
            writeString(out, value);
            out.writeLong(expiryEpochMillis);
        });
    }

    public void replicateDelete(String key) throws IOException {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        send(out -> {
            out.writeByte(DELETE);
            writeString(out, key);
        });
    }

    /**
     * Executes a remote GET query across TCP.
     * Returns null if key is missing, deleted, or expired.
     */
    public String replicateGet(String key) throws IOException {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        return sendQuery(out -> {
            out.writeByte(GET);
            writeString(out, key);
        }, in -> {
            int status = in.readUnsignedByte();
            if (status == 0) {
                return null;
            } else if (status == 1) {
                return readString(in);
            } else {
                throw new IOException("Unexpected response status from replication server: " + status);
            }
        });
    }

    private void send(Writer writer) throws IOException {
        sendQuery(writer, in -> {
            int response = in.readUnsignedByte();
            if (response != 1) {
                throw new IOException(
                        "Replication server rejected the operation (code: " + response + ")");
            }
            return null;
        });
    }

    private <T> T sendQuery(Writer writer, QueryReader<T> reader) throws IOException {
        IOException lastError = null;

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            if (attempt > 0) {
                try {
                    Thread.sleep(RETRY_BACKOFF_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Replication retry interrupted", e);
                }
            }

            try (Socket socket = new Socket()) {
                socket.connect(
                        new InetSocketAddress(host, port),
                        CONNECT_TIMEOUT_MS);
                socket.setSoTimeout(READ_TIMEOUT_MS);

                try (DataOutputStream out = new DataOutputStream(
                             new BufferedOutputStream(
                                     socket.getOutputStream()));
                     DataInputStream in = new DataInputStream(
                             new BufferedInputStream(
                                     socket.getInputStream()))) {

                    writer.write(out);
                    out.flush();

                    return reader.read(in);
                }
            } catch (IOException e) {
                lastError = e;
            }
        }

        throw new IOException(
                "Replication to " + host + ":" + port + " failed after "
                        + (maxRetries + 1) + " attempts: "
                        + (lastError != null ? lastError.getMessage() : "unknown"),
                lastError);
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

    private static String readString(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > MAX_STRING_BYTES) {
            throw new IOException("Invalid string length: " + length);
        }
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @FunctionalInterface
    private interface Writer {
        void write(DataOutputStream out) throws IOException;
    }

    @FunctionalInterface
    private interface QueryReader<T> {
        T read(DataInputStream in) throws IOException;
    }
}