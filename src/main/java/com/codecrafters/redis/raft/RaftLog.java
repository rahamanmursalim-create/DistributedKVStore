
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
    private long baseIndex;
    private long baseTerm;

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

        RaftLogEntry entry = new RaftLogEntry(baseIndex + entries.size() + 1L, term, command);

        appendDurably(appendRecord(entry));
        entries.add(entry);
        return entry;
    }

    public synchronized Optional<RaftLogEntry> get(long index) {
        if (index <= baseIndex || index > lastIndex()) {
            return Optional.empty();
        }

        return Optional.of(entries.get((int) (index - baseIndex - 1)));
    }

    public synchronized List<RaftLogEntry> entriesFrom(long index) {
        if (index <= baseIndex || index > lastIndex() + 1L) {
            throw new IllegalArgumentException("Invalid log index");
        }

        return List.copyOf(
                entries.subList((int) (index - baseIndex - 1), entries.size()));
    }

    public synchronized void truncateFrom(long index) {
        if (index <= baseIndex || index > lastIndex() + 1L) {
            throw new IllegalArgumentException("Invalid log index");
        }

        if (index <= lastIndex()) {
            appendDurably(truncateRecord(index));
            entries.subList((int) (index - baseIndex - 1), entries.size()).clear();
        }
    }

    public synchronized long lastIndex() {
        return baseIndex + entries.size();
    }

    public synchronized long lastTerm() {
        return entries.isEmpty()
                ? baseTerm
                : entries.get(entries.size() - 1).getTerm();
    }

    public synchronized int size() {
        return entries.size();
    }

    public synchronized long baseIndex() { return baseIndex; }
    public synchronized long baseTerm() { return baseTerm; }

    /** Rewrites the durable suffix after a stable state-machine snapshot. */
    public synchronized void compactThrough(long index, long term) throws IOException {
        if (index < baseIndex || index > lastIndex()) throw new IllegalArgumentException("Invalid compaction index");
        if (index == baseIndex) return;
        long actualTerm = index == baseIndex ? baseTerm : get(index).orElseThrow().getTerm();
        if (actualTerm != term) throw new IllegalArgumentException("Snapshot term does not match log");
        int remove = (int) (index - baseIndex);
        List<RaftLogEntry> suffix = new ArrayList<>(entries.subList(remove, entries.size()));
        baseIndex = index; baseTerm = term; entries.clear(); entries.addAll(suffix);
        rewrite();
    }

    /** Installs a remote snapshot boundary, retaining a suffix only when its term matches. */
    public synchronized void installSnapshot(long index, long term) throws IOException {
        if (index < baseIndex) return;
        boolean matching = index == baseIndex && term == baseTerm
                || get(index).map(entry -> entry.getTerm() == term).orElse(false);
        if (matching) {
            int remove = (int) (index - baseIndex);
            entries.subList(0, remove).clear();
        } else entries.clear();
        baseIndex = index; baseTerm = term;
        rewrite();
    }

    private void rewrite() throws IOException {
        if (file == null) return;
        if (channel != null) { channel.close(); channel = null; }
        Path temporary = file.resolveSibling(file.getFileName() + ".compact.tmp");
        try (FileChannel output = FileChannel.open(temporary, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            writeFully(output, snapshotRecord(baseIndex, baseTerm));
            for (RaftLogEntry entry : entries) writeFully(output, appendRecord(entry));
            delayForTest(); output.force(true);
        }
        try { Files.move(temporary, file, java.nio.file.StandardCopyOption.ATOMIC_MOVE,
                java.nio.file.StandardCopyOption.REPLACE_EXISTING); }
        catch (java.nio.file.AtomicMoveNotSupportedException e) {
            Files.move(temporary, file, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        }
        channel = FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
    }

    private static void writeFully(FileChannel channel, String record) throws IOException {
        ByteBuffer buffer = StandardCharsets.UTF_8.encode(record);
        while (buffer.hasRemaining()) channel.write(buffer);
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
                    if (index != baseIndex + entries.size() + 1L)
                        throw new IOException("Raft log indexes are not contiguous");
                    entries.add(new RaftLogEntry(index, term, command));
                } else if ("T".equals(fields[0]) && fields.length == 3) {
                    long index = Long.parseLong(fields[1]);
                    if (index <= baseIndex || index > lastIndex() + 1L)
                        throw new IOException("Invalid Raft log truncation index");
                    entries.subList((int) (index - baseIndex - 1), entries.size()).clear();
                } else if ("S".equals(fields[0]) && fields.length == 4 && entries.isEmpty() && baseIndex == 0) {
                    baseIndex = Long.parseLong(fields[1]); baseTerm = Long.parseLong(fields[2]);
                    if (baseIndex < 0 || baseTerm < 0 || (baseIndex == 0 && baseTerm != 0))
                        throw new IOException("Invalid Raft snapshot boundary");
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

    private static String snapshotRecord(long index, long term) {
        String body = "S\t" + index + "\t" + term;
        return body + "\t" + checksum(body) + "\n";
    }

    private static String checksum(String value) {
        CRC32 crc = new CRC32();
        crc.update(value.getBytes(StandardCharsets.UTF_8));
        return String.format("%08x", crc.getValue());
    }

    private static void delayForTest() throws IOException {
        long delay = Long.getLong("kv.test.forceDelayMillis", 0L);
        if (delay <= 0) return;
        try { Thread.sleep(delay); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IOException("Interrupted during simulated disk delay", e); }
    }

    private void appendDurably(String record) {
        if (file == null) return;
        if (channel == null) throw new IllegalStateException("Raft log is closed");
        try {
            ByteBuffer buffer = StandardCharsets.UTF_8.encode(record);
            while (buffer.hasRemaining()) channel.write(buffer);
            delayForTest(); channel.force(true);
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
