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
    private static final int MAX_BULK_STRING_BYTES = 1024 * 1024;
    private static final int MAX_ARRAY_ELEMENTS = 10_000;
    private static final int MAX_TOTAL_ARRAY_ELEMENTS = 100_000;
    private static final int MAX_ARRAY_DEPTH = 32;
    private static final long MAX_TOTAL_BULK_BYTES = 8L * 1024 * 1024;
    private static final long MAX_REQUEST_WIRE_BYTES = 16L * 1024 * 1024;
    private final InputStream in;

    private static final class Budget {
        private int elements;
        private long bulkBytes;
        private long wireBytes;
        private void addElements(int count) throws IOException {
            if (count > MAX_TOTAL_ARRAY_ELEMENTS - elements)
                throw new IOException("RESP request exceeds total array element limit");
            elements += count;
        }
        private void addBulkBytes(long count) throws IOException {
            if (count > MAX_TOTAL_BULK_BYTES - bulkBytes)
                throw new IOException("RESP request exceeds total bulk string limit");
            bulkBytes += count;
        }
        private void addWireBytes(long count) throws IOException {
            if (count > MAX_REQUEST_WIRE_BYTES - wireBytes)
                throw new IOException("RESP request exceeds maximum size");
            wireBytes += count;
        }
    }

    public RespReader(InputStream in) {
        this.in = in;
    }

    /**
     * Reads the next RESP value from the stream.
     * Returns null if the stream reaches EOF before any byte is read.
     * Throws IOException or EOFException if an error or unexpected EOF occurs.
     */
    public RespValue read() throws IOException {
        return read(0, new Budget());
    }

    private RespValue read(int depth, Budget budget) throws IOException {
        int firstByte = in.read();
        if (firstByte == -1) {
            return null; // Clean EOF
        }

        char prefix = (char) firstByte;
        switch (prefix) {
            case '+':
                return RespValue.simpleString(readLine(budget));
            case '-':
                return RespValue.error(readLine(budget));
            case ':':
                String intStr = readLine(budget);
                return RespValue.integer(Long.parseLong(intStr));
            case '$':
                return readBulkString(budget);
            case '*':
                return readArray(depth, budget);
            default:
                // Handle inline command (e.g. "PING\r\n" or "SET foo bar\r\n")
                return readInlineCommand(firstByte, budget);
        }
    }

    private RespValue readBulkString(Budget budget) throws IOException {
        String lengthLine = readLine(budget);
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
        budget.addBulkBytes(length);
        budget.addWireBytes(length + 2L);

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

        return RespValue.bulkString(buffer);
    }

    private RespValue readArray(int depth, Budget budget) throws IOException {
        String countLine = readLine(budget);
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
        if (depth >= MAX_ARRAY_DEPTH) throw new IOException("RESP array nesting exceeds maximum depth");
        budget.addElements(count);

        List<RespValue> elements = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            RespValue elem = read(depth + 1, budget);
            if (elem == null) {
                throw new EOFException("Unexpected EOF while reading array element " + i + " of " + count);
            }
            elements.add(elem);
        }
        return RespValue.array(elements);
    }

    private RespValue readInlineCommand(int firstByte, Budget budget) throws IOException {
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
            if (buffer.size() > MAX_LINE_BYTES)
                throw new IOException("RESP line exceeds maximum size");
        }

        budget.addWireBytes(buffer.size() + 2L);
        String line = buffer.toString(StandardCharsets.UTF_8).trim();
        if (line.isEmpty()) {
            return read(0, budget); // empty line, read next
        }

        String[] parts = line.split("\\s+");
        budget.addElements(parts.length);
        List<RespValue> elements = new ArrayList<>();
        for (String part : parts) {
            budget.addBulkBytes(part.getBytes(StandardCharsets.UTF_8).length);
            elements.add(RespValue.bulkString(part));
        }
        return RespValue.array(elements);
    }

    /**
     * Reads a line ending with \r\n or \n.
     */
    private String readLine(Budget budget) throws IOException {
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
        budget.addWireBytes(buffer.size() + 2L);
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
