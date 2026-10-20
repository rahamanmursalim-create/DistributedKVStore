package com.codecrafters.redis.raft;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

public final class RaftLogPersistenceTest {
    public static void main(String[] args) throws Exception {
        Path directory = Files.createTempDirectory("raft-log-persistence");
        Path file = directory.resolve("raft.log");
        try {
            try (RaftLog log = new RaftLog(file)) {
                log.append(1, new byte[] {1, 2, 3});
                log.append(1, "second".getBytes(java.nio.charset.StandardCharsets.UTF_8));
                log.append(2, new byte[] {0, 9, 10});
                log.truncateFrom(3);
                log.append(2, "replacement".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }

            try (RaftLog recovered = new RaftLog(file)) {
                assert recovered.size() == 3;
                assert recovered.get(1).orElseThrow().getTerm() == 1;
                assert recovered.get(2).orElseThrow().getTerm() == 1;
                assert Arrays.equals(recovered.get(3).orElseThrow().getCommand(),
                        "replacement".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }

            // An incomplete final append must be ignored and removed before reuse.
            Files.writeString(file, "A\t4\t2\tpartial", StandardOpenOption.APPEND);
            try (RaftLog recovered = new RaftLog(file)) {
                assert recovered.size() == 3;
                recovered.append(3, "after-recovery".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }
            try (RaftLog recovered = new RaftLog(file)) {
                assert recovered.size() == 4;
                assert new String(recovered.get(4).orElseThrow().getCommand(),
                        java.nio.charset.StandardCharsets.UTF_8).equals("after-recovery");
            }

            Path corrupt = directory.resolve("corrupt.log");
            Files.writeString(corrupt, "A\t1\t1\tYQ\t00000000\n");
            boolean rejected = false;
            try { new RaftLog(corrupt); }
            catch (java.io.IOException expected) { rejected = true; }
            assert rejected : "corrupt durable Raft records must be rejected";
            System.out.println("RaftLogPersistenceTest passed");
        } finally {
            try (var paths = Files.walk(directory)) {
                paths.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
                    try { Files.deleteIfExists(path); }
                    catch (Exception e) { throw new RuntimeException(e); }
                });
            }
        }
    }
}
