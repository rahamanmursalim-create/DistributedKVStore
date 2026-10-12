package com.codecrafters.redis.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Append-only write-ahead log.
 *
 * Each complete line is:
 * PUT<TAB>base64(key)<TAB>base64(value)<TAB>expiryEpochMillis
 * DEL<TAB>base64(key)
 *
 * A mutation is forced to disk before RedisStore changes its in-memory map.
 */
public final class WriteAheadLog implements AutoCloseable {
    private static final String PUT = "PUT";
    private static final String DEL = "DEL";

    private final Path path;
    private FileChannel channel;

    public WriteAheadLog(Path path) throws IOException {
        this.path = path.toAbsolutePath().normalize();
        Path parent = this.path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        if (!Files.exists(this.path)) {
            Files.createFile(this.path);
        }
        this.channel = FileChannel.open(
                this.path,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND
        );
    }

    public synchronized void appendPut(String key, String value, long expiryEpochMillis)
            throws IOException {
        String line = PUT + "\t" + encode(key) + "\t" + encode(value) + "\t"
                + expiryEpochMillis + "\n";
        appendAndSync(line);
    }

    public synchronized void appendDelete(String key) throws IOException {
        appendAndSync(DEL + "\t" + encode(key) + "\n");
    }

    private void appendAndSync(String line) throws IOException {
        ensureOpen();
        ByteBuffer buffer = StandardCharsets.UTF_8.encode(line);
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
        channel.force(true);
    }

    /**
     * Reads only newline-terminated records. A final incomplete line is ignored,
     * because a process can crash in the middle of writing the last record.
     */
    public synchronized List<Record> replay() throws IOException {
        ensureOpen();
        byte[] bytes = Files.readAllBytes(path);
        String contents = new String(bytes, StandardCharsets.UTF_8);
        int completeLength = contents.lastIndexOf('\n') + 1;

        // Remove a partial tail before future appends, otherwise the next
        // record could be concatenated to the incomplete record.
        if (completeLength < bytes.length) {
            channel.truncate(completeLength);
            channel.position(completeLength);
            channel.force(true);
        }
        if (completeLength == 0) {
            return List.of();
        }

        String completeContents = contents.substring(0, completeLength);
        List<Record> records = new ArrayList<>();
        String[] lines = completeContents.split("\n");
        for (String line : lines) {
            if (line.isEmpty()) {
                continue;
            }
            String[] fields = line.split("\t", -1);
            try {
                if (fields.length == 4 && PUT.equals(fields[0])) {
                    records.add(new Record(
                            Operation.PUT,
                            decode(fields[1]),
                            decode(fields[2]),
                            Long.parseLong(fields[3])
                    ));
                } else if (fields.length == 2 && DEL.equals(fields[0])) {
                    records.add(new Record(
                            Operation.DELETE,
                            decode(fields[1]),
                            null,
                            -1L
                    ));
                } else {
                    throw new IOException("Invalid WAL record");
                }
            } catch (IllegalArgumentException ex) {
                throw new IOException("Invalid WAL record encoding", ex);
            }
        }
        return records;
    }

    public Path getPath() {
        return path;
    }

    private static String encode(String value) {
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String decode(String value) {
        return new String(Base64.getUrlDecoder().decode(value), StandardCharsets.UTF_8);
    }

    private void ensureOpen() throws IOException {
        if (channel == null || !channel.isOpen()) {
            throw new IOException("WAL is closed");
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    public enum Operation {
        PUT, DELETE
    }

    public static final class Record {
        private final Operation operation;
        private final String key;
        private final String value;
        private final long expiryEpochMillis;

        private Record(Operation operation, String key, String value, long expiryEpochMillis) {
            this.operation = operation;
            this.key = key;
            this.value = value;
            this.expiryEpochMillis = expiryEpochMillis;
        }

        public Operation getOperation() {
            return operation;
        }

        public String getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }

        public long getExpiryEpochMillis() {
            return expiryEpochMillis;
        }
    }
}
