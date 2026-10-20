
package com.codecrafters.redis.raft;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.zip.CRC32;

public final class RaftLog implements AutoCloseable {

    private final List<RaftLogEntry> entries = new ArrayList<>();
    private final Path file;
    private FileChannel channel;

    /** Creates a memory-only log, useful for ephemeral groups and tests. */
    public RaftLog() {
        this.file = null;
    }

    /** Opens a durable append-only Raft log and recovers it before returning. */
    public RaftLog(Path file) throws IOException {
        this.file = file.toAbsolutePath().normalize();
        Path parent = this.file.getParent();
        if (parent != null) Files.createDirectories(parent);
        if (!Files.exists(this.file)) Files.createFile(this.file);
        recover();
        this.channel = FileChannel.open(this.file,
                StandardOpenOption.WRITE, StandardOpenOption.APPEND);
    }

    public synchronized RaftLogEntry append(
            long term, byte[] command) {

        RaftLogEntry entry = new RaftLogEntry(
                entries.size() + 1L, term, command);

        appendDurably(appendRecord(entry));
        entries.add(entry);
        return entry;
    }

    public synchronized Optional<RaftLogEntry> get(long index) {
        if (index <= 0 || index > entries.size()) {
            return Optional.empty();
        }

        return Optional.of(entries.get((int) index - 1));
    }

    public synchronized List<RaftLogEntry> entriesFrom(long index) {
        if (index < 1 || index > entries.size() + 1L) {
            throw new IllegalArgumentException("Invalid log index");
        }

        return List.copyOf(
                entries.subList((int) index - 1, entries.size()));
    }

    public synchronized void truncateFrom(long index) {
        if (index < 1 || index > entries.size() + 1L) {
            throw new IllegalArgumentException("Invalid log index");
        }

        if (index <= entries.size()) {
            appendDurably(truncateRecord(index));
            entries.subList((int) index - 1, entries.size()).clear();
        }
    }

    public synchronized long lastIndex() {
        return entries.size();
    }

    public synchronized long lastTerm() {
        return entries.isEmpty()
                ? 0
                : entries.get(entries.size() - 1).getTerm();
    }

    public synchronized int size() {
        return entries.size();
    }

    private void recover() throws IOException {
        byte[] bytes = Files.readAllBytes(file);
        String contents = new String(bytes, StandardCharsets.UTF_8);
        int completeLength = contents.lastIndexOf('\n') + 1;
        if (completeLength < bytes.length) {
            try (FileChannel repair = FileChannel.open(file,
                    StandardOpenOption.WRITE)) {
                repair.truncate(completeLength);
                repair.force(true);
            }
            contents = contents.substring(0, completeLength);
        }
        if (contents.isEmpty()) return;

        for (String line : contents.split("\n")) {
            if (line.isEmpty()) continue;
            String[] fields = line.split("\t", -1);
            if (fields.length < 3) throw new IOException("Invalid Raft log record");
            String body = line.substring(0, line.lastIndexOf('\t'));
            if (!checksum(body).equals(fields[fields.length - 1]))
                throw new IOException("Raft log checksum mismatch");

            try {
                if ("A".equals(fields[0]) && fields.length == 5) {
                    long index = Long.parseLong(fields[1]);
                    long term = Long.parseLong(fields[2]);
                    byte[] command = Base64.getUrlDecoder().decode(fields[3]);
                    if (index != entries.size() + 1L)
                        throw new IOException("Raft log indexes are not contiguous");
                    entries.add(new RaftLogEntry(index, term, command));
                } else if ("T".equals(fields[0]) && fields.length == 3) {
                    long index = Long.parseLong(fields[1]);
                    if (index < 1 || index > entries.size() + 1L)
                        throw new IOException("Invalid Raft log truncation index");
                    entries.subList((int) index - 1, entries.size()).clear();
                } else {
                    throw new IOException("Unknown Raft log record");
                }
            } catch (IllegalArgumentException e) {
                throw new IOException("Invalid Raft log record encoding", e);
            }
        }
    }

    private static String appendRecord(RaftLogEntry entry) {
        String body = "A\t" + entry.getIndex() + "\t" + entry.getTerm() + "\t"
                + Base64.getUrlEncoder().withoutPadding().encodeToString(entry.getCommand());
        return body + "\t" + checksum(body) + "\n";
    }

    private static String truncateRecord(long index) {
        String body = "T\t" + index;
        return body + "\t" + checksum(body) + "\n";
    }

    private static String checksum(String value) {
        CRC32 crc = new CRC32();
        crc.update(value.getBytes(StandardCharsets.UTF_8));
        return String.format("%08x", crc.getValue());
    }

    private void appendDurably(String record) {
        if (file == null) return;
        if (channel == null) throw new IllegalStateException("Raft log is closed");
        try {
            ByteBuffer buffer = StandardCharsets.UTF_8.encode(record);
            while (buffer.hasRemaining()) channel.write(buffer);
            channel.force(true);
        } catch (IOException e) {
            throw new UncheckedIOException("Could not persist Raft log record", e);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }
}
