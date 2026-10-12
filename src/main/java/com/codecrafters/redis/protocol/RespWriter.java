package com.codecrafters.redis.protocol;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Utility class to write RESP encoded data to an OutputStream.
 */
public final class RespWriter {
    private static final byte[] CRLF = "\r\n".getBytes(StandardCharsets.UTF_8);
    private static final byte[] NULL_BULK_STRING_BYTES = "$-1\r\n".getBytes(StandardCharsets.UTF_8);
    private static final byte[] OK_BYTES = "+OK\r\n".getBytes(StandardCharsets.UTF_8);
    private static final byte[] PONG_BYTES = "+PONG\r\n".getBytes(StandardCharsets.UTF_8);

    private RespWriter() {}

    public static void write(OutputStream out, RespValue value) throws IOException {
        if (value == null) {
            writeNullBulkString(out);
            return;
        }
        out.write(value.encode());
    }

    public static void writeSimpleString(OutputStream out, String str) throws IOException {
        if ("OK".equals(str)) {
            out.write(OK_BYTES);
        } else if ("PONG".equals(str)) {
            out.write(PONG_BYTES);
        } else {
            out.write(('+' + str + "\r\n").getBytes(StandardCharsets.UTF_8));
        }
    }

    public static void writeError(OutputStream out, String message) throws IOException {
        out.write(('-' + message + "\r\n").getBytes(StandardCharsets.UTF_8));
    }

    public static void writeInteger(OutputStream out, long value) throws IOException {
        out.write((':' + String.valueOf(value) + "\r\n").getBytes(StandardCharsets.UTF_8));
    }

    public static void writeBulkString(OutputStream out, String str) throws IOException {
        if (str == null) {
            writeNullBulkString(out);
            return;
        }
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        String header = "$" + bytes.length + "\r\n";
        out.write(header.getBytes(StandardCharsets.UTF_8));
        out.write(bytes);
        out.write(CRLF);
    }

    public static void writeNullBulkString(OutputStream out) throws IOException {
        out.write(NULL_BULK_STRING_BYTES);
    }

    public static void writeArray(OutputStream out, List<RespValue> elements) throws IOException {
        if (elements == null) {
            out.write("*-1\r\n".getBytes(StandardCharsets.UTF_8));
            return;
        }
        String header = "*" + elements.size() + "\r\n";
        out.write(header.getBytes(StandardCharsets.UTF_8));
        for (RespValue elem : elements) {
            write(out, elem);
        }
    }
}
