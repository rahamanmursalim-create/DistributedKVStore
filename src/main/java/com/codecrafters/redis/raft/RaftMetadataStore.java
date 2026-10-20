package com.codecrafters.redis.raft;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.zip.CRC32;

/** Atomically persisted Raft currentTerm and votedFor state. */
final class RaftMetadataStore {
    record State(long term, String votedFor, long commitIndex) {}

    private final Path path;

    RaftMetadataStore(Path path) { this.path = path; }

    synchronized State load() throws IOException {
        if (!Files.exists(path)) return new State(0, null, 0);
        String line = Files.readString(path, StandardCharsets.UTF_8).trim();
        String[] fields = line.split("\\t", -1);
        if (fields.length != 3 && fields.length != 4) throw new IOException("Invalid Raft metadata");
        String body = fields.length == 3 ? fields[0] + "\t" + fields[1]
                : fields[0] + "\t" + fields[1] + "\t" + fields[2];
        if (!checksum(body).equals(fields[fields.length - 1])) throw new IOException("Raft metadata checksum mismatch");
        try {
            long term = Long.parseLong(fields[0]);
            if (term < 0) throw new NumberFormatException("negative term");
            String vote = fields[1].isEmpty() ? null
                    : new String(Base64.getUrlDecoder().decode(fields[1]), StandardCharsets.UTF_8);
            long commitIndex = fields.length == 4 ? Long.parseLong(fields[2]) : 0;
            if (commitIndex < 0) throw new NumberFormatException("negative commit index");
            return new State(term, vote, commitIndex);
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid Raft metadata encoding", e);
        }
    }

    synchronized void save(long term, String votedFor) throws IOException {
        save(term, votedFor, 0);
    }

    synchronized void save(long term, String votedFor, long commitIndex) throws IOException {
        if (term < 0) throw new IllegalArgumentException("Term cannot be negative");
        if (commitIndex < 0) throw new IllegalArgumentException("Commit index cannot be negative");
        Path parent = path.getParent();
        if (parent != null) Files.createDirectories(parent);
        String vote = votedFor == null ? ""
                : Base64.getUrlEncoder().withoutPadding()
                    .encodeToString(votedFor.getBytes(StandardCharsets.UTF_8));
        String body = term + "\t" + vote + "\t" + commitIndex;
        byte[] bytes = (body + "\t" + checksum(body) + "\n").getBytes(StandardCharsets.UTF_8);
        Path temporary = path.resolveSibling(path.getFileName() + ".tmp");
        try (FileChannel channel = FileChannel.open(temporary,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE)) {
            java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(bytes);
            while (buffer.hasRemaining()) channel.write(buffer);
            channel.force(true);
        }
        try {
            Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (java.nio.file.AtomicMoveNotSupportedException e) {
            Files.move(temporary, path, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static String checksum(String value) {
        CRC32 crc = new CRC32();
        crc.update(value.getBytes(StandardCharsets.UTF_8));
        return String.format("%08x", crc.getValue());
    }
}
