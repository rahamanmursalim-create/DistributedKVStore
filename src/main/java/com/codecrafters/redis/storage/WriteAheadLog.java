package com.codecrafters.redis.storage;

import java.io.IOException;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.nio.file.AccessDeniedException;
import java.util.zip.CRC32;

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
    private static final String PUT_CHECKSUM = "PUT2";
    private static final String DEL_CHECKSUM = "DEL2";
    private static final String TXN_CHECKSUM = "TXN2";
    private static final int MAX_RECORD_BYTES = 128 * 1024 * 1024;

    private final Path path;
    private FileChannel channel;

    public WriteAheadLog(Path path) throws IOException {
        this.path = path.toAbsolutePath().normalize();
        Path parent = this.path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        recoverInterruptedReplacement(this.path);
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
        String body = PUT_CHECKSUM + "\t" + encode(key) + "\t"
                + encode(value) + "\t" + expiryEpochMillis;
        appendAndSync(body + "\t" + checksum(body) + "\n");
    }

    /** Appends several PUT records and forces the WAL once for the entire batch. */
    public synchronized void appendPuts(List<PutRecord> records) throws IOException {
        ensureOpen();
        if (records == null || records.isEmpty()) return;
        StringBuilder batch = new StringBuilder();
        for (PutRecord record : records) {
            String body = PUT_CHECKSUM + "\t" + encode(record.key) + "\t"
                    + encode(record.value) + "\t" + record.expiryEpochMillis;
            batch.append(body).append('\t').append(checksum(body)).append('\n');
        }
        ByteBuffer buffer = StandardCharsets.UTF_8.encode(batch.toString());
        while (buffer.hasRemaining()) channel.write(buffer);
        delayForTest(); channel.force(true);
    }

    /** Appends a complete atomic transaction as one checksummed WAL record. */
    public synchronized void appendTransaction(java.util.Map<String, String> changes)
            throws IOException {
        ensureOpen();
        if (changes == null || changes.isEmpty()) return;
        StringBuilder payload = new StringBuilder();
        for (java.util.Map.Entry<String, String> change : changes.entrySet()) {
            if (change.getKey() == null) throw new IllegalArgumentException("Key cannot be null");
            if (change.getValue() == null) {
                payload.append("D\t").append(encode(change.getKey())).append('\n');
            } else {
                payload.append("P\t").append(encode(change.getKey())).append('\t')
                        .append(encode(change.getValue())).append('\n');
            }
        }
        String body = TXN_CHECKSUM + "\t" + encode(payload.toString());
        appendAndSync(body + "\t" + checksum(body) + "\n");
    }

    public synchronized void appendDelete(String key) throws IOException {
        String body = DEL_CHECKSUM + "\t" + encode(key);
        appendAndSync(body + "\t" + checksum(body) + "\n");
    }

    /** Atomically replaces WAL history with the supplied live snapshot. */
    public synchronized void replaceWithSnapshot(java.util.Map<String, ValueEntry> entries) throws IOException {
        ensureOpen();
        Path temporary = path.resolveSibling(path.getFileName() + ".snapshot.tmp");
        Path backup = path.resolveSibling(path.getFileName() + ".snapshot.bak");
        Path backupTemporary = path.resolveSibling(path.getFileName() + ".snapshot.bak.tmp");
        Path marker = path.resolveSibling(path.getFileName() + ".snapshot.commit");
        Path backupReady = path.resolveSibling(path.getFileName() + ".snapshot.backup-ready");
        try (FileChannel output = FileChannel.open(temporary, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            for (java.util.Map.Entry<String, ValueEntry> entry : entries.entrySet()) {
                if (entry.getKey() == null || entry.getValue() == null) throw new IllegalArgumentException("Null snapshot entry");
                String body = PUT_CHECKSUM + "\t" + encode(entry.getKey()) + "\t"
                        + encode(entry.getValue().getValue()) + "\t" + entry.getValue().getExpiryTimestampMillis();
                ByteBuffer buffer = StandardCharsets.UTF_8.encode(body + "\t" + checksum(body) + "\n");
                while (buffer.hasRemaining()) output.write(buffer);
            }
            delayForTest(); output.force(true);
        }
        forceDirectory(path.getParent());
        channel.close(); channel = null;
        IOException failure = null;
        try {
            try {
                Files.move(temporary, path, java.nio.file.StandardCopyOption.ATOMIC_MOVE,
                        java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            } catch (java.nio.file.AtomicMoveNotSupportedException unsupported) {
                Files.copy(path, backupTemporary, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                forceFile(backupTemporary);
                forceDirectory(path.getParent());
                writeMarker(backupReady, "WAL-SNAPSHOT-BACKUP-2");
                forceDirectory(path.getParent());
                Files.move(temporary, path, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                forceFile(path);
                writeMarker(marker, "WAL-SNAPSHOT-2");
                forceDirectory(path.getParent());
                Files.deleteIfExists(backupTemporary);
                Files.deleteIfExists(backup);
                Files.deleteIfExists(backupReady);
                forceDirectory(path.getParent());
                Files.deleteIfExists(marker);
                forceDirectory(path.getParent());
            }
            forceDirectory(path.getParent());
        } catch (IOException replacementFailure) {
            try { recoverInterruptedReplacement(path); }
            catch (IOException recoveryFailure) { replacementFailure.addSuppressed(recoveryFailure); }
            failure = replacementFailure;
        }
        try {
            channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        } catch (IOException reopenFailure) {
            if (failure == null) failure = reopenFailure;
            else failure.addSuppressed(reopenFailure);
        }
        if (failure != null) throw failure;
    }

    private void appendAndSync(String line) throws IOException {
        ensureOpen();
        ByteBuffer buffer = StandardCharsets.UTF_8.encode(line);
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
        delayForTest(); channel.force(true);
    }

    /**
     * Reads only newline-terminated records. A final incomplete line is ignored,
     * because a process can crash in the middle of writing the last record.
     */
    public synchronized List<Record> replay() throws IOException {
        List<Record> records = new ArrayList<>();
        replay(records::add);
        return records;
    }

    /** Streams each recovered record to the state machine without retaining WAL history. */
    public synchronized void replay(RecordConsumer consumer) throws IOException {
        ensureOpen();
        java.util.Objects.requireNonNull(consumer, "consumer");
        long completeOffset = 0;
        java.io.ByteArrayOutputStream lineBytes = new java.io.ByteArrayOutputStream();
        try (InputStream input = new BufferedInputStream(Files.newInputStream(path))) {
            int next;
            while ((next = input.read()) != -1) {
                if (next == '\n') {
                    if (lineBytes.size() > 0)
                        replayLine(lineBytes.toString(StandardCharsets.UTF_8), consumer);
                    completeOffset += lineBytes.size() + 1L;
                    lineBytes.reset();
                } else {
                    if (lineBytes.size() >= MAX_RECORD_BYTES)
                        throw new IOException("WAL record exceeds maximum supported size");
                    lineBytes.write(next);
                }
            }
        }
        // Ignore and durably remove a torn tail before any future append.
        if (lineBytes.size() > 0) {
            channel.truncate(completeOffset);
            channel.position(completeOffset);
            delayForTest(); channel.force(true);
        }
    }

    private static void replayLine(String line, RecordConsumer consumer) throws IOException {
        String[] fields = line.split("\t", -1);
        try {
            if (fields.length == 5 && PUT_CHECKSUM.equals(fields[0])) {
                verifyChecksum(line, fields[4]);
                consumer.accept(new Record(Operation.PUT, decode(fields[1]), decode(fields[2]),
                        Long.parseLong(fields[3])));
            } else if (fields.length == 3 && TXN_CHECKSUM.equals(fields[0])) {
                verifyChecksum(line, fields[2]);
                replayTransaction(fields[1], consumer);
            } else if (fields.length == 3 && DEL_CHECKSUM.equals(fields[0])) {
                verifyChecksum(line, fields[2]);
                consumer.accept(new Record(Operation.DELETE, decode(fields[1]), null, -1L));
            } else if (fields.length == 4 && PUT.equals(fields[0])) {
                consumer.accept(new Record(Operation.PUT, decode(fields[1]), decode(fields[2]),
                        Long.parseLong(fields[3])));
            } else if (fields.length == 2 && DEL.equals(fields[0])) {
                consumer.accept(new Record(Operation.DELETE, decode(fields[1]), null, -1L));
            } else {
                throw new IOException("Invalid WAL record");
            }
        } catch (IllegalArgumentException ex) {
            throw new IOException("Invalid WAL record encoding", ex);
        }
    }

    private static void replayTransaction(String encodedPayload, RecordConsumer consumer) throws IOException {
        byte[] encoded = encodedPayload.getBytes(StandardCharsets.US_ASCII);
        try (InputStream decoded = new BufferedInputStream(Base64.getUrlDecoder().wrap(new ByteArrayInputStream(encoded)))) {
            while (true) {
                String operation = readTransactionLine(decoded);
                if (operation == null) break;
                if (operation.isEmpty()) continue;
                String[] parts = operation.split("\t", -1);
                try {
                    if (parts.length == 2 && "D".equals(parts[0])) {
                        consumer.accept(new Record(Operation.DELETE, decode(parts[1]), null, -1L));
                    } else if (parts.length == 3 && "P".equals(parts[0])) {
                        consumer.accept(new Record(Operation.PUT, decode(parts[1]), decode(parts[2]), -1L));
                    } else {
                        throw new IOException("Invalid WAL transaction record");
                    }
                } catch (IllegalArgumentException ex) {
                    throw new IOException("Invalid WAL transaction encoding", ex);
                }
            }
        } catch (IllegalArgumentException ex) {
            throw new IOException("Invalid WAL transaction encoding", ex);
        }
    }

    private static String readTransactionLine(InputStream input) throws IOException {
        java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
        int next;
        while ((next = input.read()) != -1) {
            if (next == '\n') return bytes.toString(StandardCharsets.UTF_8);
            if (bytes.size() >= MAX_RECORD_BYTES) throw new IOException("WAL transaction operation exceeds maximum size");
            bytes.write(next);
        }
        if (bytes.size() == 0) return null;
        throw new IOException("Truncated WAL transaction payload");
    }

    @FunctionalInterface
    public interface RecordConsumer { void accept(Record record) throws IOException; }

    private static void recoverInterruptedReplacement(Path path) throws IOException {
        Path backup = path.resolveSibling(path.getFileName() + ".snapshot.bak");
        Path backupTemporary = path.resolveSibling(path.getFileName() + ".snapshot.bak.tmp");
        Path temporary = path.resolveSibling(path.getFileName() + ".snapshot.tmp");
        Path marker = path.resolveSibling(path.getFileName() + ".snapshot.commit");
        Path backupReady = path.resolveSibling(path.getFileName() + ".snapshot.backup-ready");
        boolean committed = markerEquals(marker, "WAL-SNAPSHOT-2");
        boolean hasReadyBackup = markerEquals(backupReady, "WAL-SNAPSHOT-BACKUP-2");
        if (committed && Files.exists(path)) {
            Files.deleteIfExists(backup);
            Files.deleteIfExists(backupTemporary);
        } else if (hasReadyBackup && Files.exists(backupTemporary)) {
            Files.move(backupTemporary, path, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        } else if (Files.exists(backup)) {
            if (!committed || !Files.exists(path))
                Files.move(backup, path, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            else Files.deleteIfExists(backup);
        } else if (!Files.exists(path) && (Files.exists(marker) || Files.exists(temporary)
                || Files.exists(backupReady) || Files.exists(backupTemporary))) {
            throw new IOException("WAL replacement recovery has no valid active or backup file");
        }
        Files.deleteIfExists(temporary);
        Files.deleteIfExists(backupTemporary);
        Files.deleteIfExists(marker);
        Files.deleteIfExists(backupReady);
        forceDirectory(path.getParent());
    }

    private static void writeMarker(Path marker, String contents) throws IOException {
        try (FileChannel output = FileChannel.open(marker, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            ByteBuffer bytes = StandardCharsets.UTF_8.encode(contents);
            while (bytes.hasRemaining()) output.write(bytes);
            output.force(true);
        }
    }

    private static boolean markerEquals(Path marker, String expected) {
        if (!Files.exists(marker)) return false;
        try { return expected.equals(Files.readString(marker, StandardCharsets.UTF_8)); }
        catch (IOException ignored) { return false; }
    }

    private static void forceFile(Path file) throws IOException {
        try (FileChannel output = FileChannel.open(file, StandardOpenOption.WRITE)) { output.force(true); }
    }

    private static void forceDirectory(Path directory) throws IOException {
        if (directory == null) return;
        try (FileChannel output = FileChannel.open(directory, StandardOpenOption.READ)) { output.force(true); }
        catch (AccessDeniedException | UnsupportedOperationException unsupported) {
            // Some platforms do not expose directory fsync through FileChannel.
        }
    }

    /** Returns the current WAL length for bounded automatic compaction policies. */
    public synchronized long sizeBytes() throws IOException {
        ensureOpen();
        return channel.size();
    }

    public Path getPath() {
        return path;
    }

    private static String checksum(String payload) {
        CRC32 crc = new CRC32();
        crc.update(payload.getBytes(StandardCharsets.UTF_8));
        return String.format("%08x", crc.getValue());
    }

    private static void verifyChecksum(String line, String expected)
            throws IOException {
        int separator = line.lastIndexOf('\t');
        if (separator < 0 || !checksum(line.substring(0, separator)).equals(expected)) {
            throw new IOException("WAL checksum mismatch");
        }
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

    private static void delayForTest() throws IOException {
        long delay = Long.getLong("kv.test.forceDelayMillis", 0L);
        if (delay <= 0) return;
        try { Thread.sleep(delay); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IOException("Interrupted during simulated disk delay", e); }
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

    public static final class PutRecord {
        private final String key;
        private final String value;
        private final long expiryEpochMillis;

        public PutRecord(String key, String value, long expiryEpochMillis) {
            if (key == null || value == null) throw new IllegalArgumentException("Key and value cannot be null");
            this.key = key;
            this.value = value;
            this.expiryEpochMillis = expiryEpochMillis;
        }
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
