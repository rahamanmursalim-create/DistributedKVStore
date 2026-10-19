package com.codecrafters.redis.protocol;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Streaming RESP (REdis Serialization Protocol) Parser.
 * Handles arrays, bulk strings, simple strings, integers, errors,
 * as well as inline commands (e.g. from netcat / telnet).
 */
public class RespReader {
    private static final int MAX_LINE_BYTES = 64 * 1024;
    private static final int MAX_BULK_STRING_BYTES = 64 * 1024 * 1024;
    private static final int MAX_ARRAY_ELEMENTS = 1_000_000;
    private final InputStream in;

    public RespReader(InputStream in) {
        this.in = in;
    }

    /**
     * Reads the next RESP value from the stream.
     * Returns null if the stream reaches EOF before any byte is read.
     * Throws IOException or EOFException if an error or unexpected EOF occurs.
     */
    public RespValue read() throws IOException {
        int firstByte = in.read();
        if (firstByte == -1) {
            return null; // Clean EOF
        }

        char prefix = (char) firstByte;
        switch (prefix) {
            case '+':
                return RespValue.simpleString(readLine());
            case '-':
                return RespValue.error(readLine());
            case ':':
                String intStr = readLine();
                return RespValue.integer(Long.parseLong(intStr));
            case '$':
                return readBulkString();
            case '*':
                return readArray();
            default:
                // Handle inline command (e.g. "PING\r\n" or "SET foo bar\r\n")
                return readInlineCommand(firstByte);
        }
    }

    private RespValue readBulkString() throws IOException {
        String lengthLine = readLine();
        int length = parseLength(lengthLine, "bulk string");
        if (length == -1) {
            return RespValue.nullBulkString();
        }
        if (length < 0) {
            throw new IOException("Invalid bulk string length: " + length);
        }
        if (length > MAX_BULK_STRING_BYTES) {
            throw new IOException("Bulk string exceeds maximum size");
        }

        byte[] buffer = readExactNBytes(length);
        // Consume trailing CRLF (\r\n)
        int cr = in.read();
        int lf = in.read();
        if (cr != '\r' || lf != '\n') {
            // In case only \n was sent or malformed CRLF
            if (cr == '\n') {
                // Single LF, unread not supported directly so we accept it
            } else {
                throw new IOException("Expected CRLF after bulk string content, got: " + cr + ", " + lf);
            }
        }

        String str = new String(buffer, StandardCharsets.UTF_8);
        return RespValue.bulkString(str);
    }

    private RespValue readArray() throws IOException {
        String countLine = readLine();
        int count = parseLength(countLine, "array");
        if (count == -1) {
            return RespValue.array(null);
        }
        if (count < 0) {
            throw new IOException("Invalid array length: " + count);
        }
        if (count > MAX_ARRAY_ELEMENTS) {
            throw new IOException("Array exceeds maximum element count");
        }

        List<RespValue> elements = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            RespValue elem = read();
            if (elem == null) {
                throw new EOFException("Unexpected EOF while reading array element " + i + " of " + count);
            }
            elements.add(elem);
        }
        return RespValue.array(elements);
    }

    private RespValue readInlineCommand(int firstByte) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        buffer.write(firstByte);

        int b;
        while ((b = in.read()) != -1) {
            if (b == '\r') {
                int next = in.read();
                if (next != '\n' && next != -1) {
                    buffer.write(b);
                    buffer.write(next);
                }
                break;
            }
            if (b == '\n') {
                break;
            }
            buffer.write(b);
        }

        String line = buffer.toString(StandardCharsets.UTF_8).trim();
        if (line.isEmpty()) {
            return read(); // empty line, read next
        }

        String[] parts = line.split("\\s+");
        List<RespValue> elements = new ArrayList<>();
        for (String part : parts) {
            elements.add(RespValue.bulkString(part));
        }
        return RespValue.array(elements);
    }

    /**
     * Reads a line ending with \r\n or \n.
     */
    private String readLine() throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\r') {
                int next = in.read();
                if (next != '\n' && next != -1) {
                    buffer.write(b);
                    buffer.write(next);
                }
                break;
            }
            if (b == '\n') {
                break;
            }
            buffer.write(b);
            if (buffer.size() > MAX_LINE_BYTES) {
                throw new IOException("RESP line exceeds maximum size");
            }
        }
        if (b == -1 && buffer.size() == 0) {
            throw new EOFException("Unexpected EOF while reading line");
        }
        return buffer.toString(StandardCharsets.UTF_8);
    }

    private static int parseLength(String value, String kind) throws IOException {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IOException("Invalid " + kind + " length: " + value, e);
        }
    }

    /**
     * Reads exactly n bytes from the input stream, blocking until all bytes are read.
     */
    private byte[] readExactNBytes(int n) throws IOException {
        byte[] bytes = new byte[n];
        int readTotal = 0;
        while (readTotal < n) {
            int count = in.read(bytes, readTotal, n - readTotal);
            if (count == -1) {
                throw new EOFException("Unexpected EOF while reading " + n + " bytes; only read " + readTotal);
            }
            readTotal += count;
        }
        return bytes;
    }
}
