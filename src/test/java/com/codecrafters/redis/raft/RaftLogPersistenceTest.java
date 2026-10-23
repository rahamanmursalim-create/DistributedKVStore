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
                recovered.compactThrough(2, 1);
                assert recovered.baseIndex() == 2;
                assert recovered.baseTerm() == 1;
                assert recovered.get(1).isEmpty();
                assert recovered.get(2).isEmpty();
                assert recovered.get(3).orElseThrow().getTerm() == 2;
                assert recovered.lastIndex() == 4;
            }
            try (RaftLog recovered = new RaftLog(file)) {
                assert recovered.baseIndex() == 2;
                assert recovered.baseTerm() == 1;
                assert recovered.lastIndex() == 4;
                recovered.append(4, "after-compaction".getBytes(java.nio.charset.StandardCharsets.UTF_8));
                assert recovered.get(5).isPresent();
            }
            try (RaftLog follower = new RaftLog(directory.resolve("follower.log"))) {
                follower.append(1, "old-1".getBytes(java.nio.charset.StandardCharsets.UTF_8));
                follower.append(2, "old-2".getBytes(java.nio.charset.StandardCharsets.UTF_8));
                follower.append(2, "suffix".getBytes(java.nio.charset.StandardCharsets.UTF_8));
                follower.installSnapshot(2, 2);
                assert follower.baseIndex() == 2 && follower.baseTerm() == 2;
                assert follower.lastIndex() == 3 && follower.get(3).isPresent();
            }
            try (RaftLog follower = new RaftLog(directory.resolve("follower.log"))) {
                assert follower.baseIndex() == 2 && follower.lastIndex() == 3;
                follower.installSnapshot(4, 9); // divergent suffix must be discarded
                assert follower.baseIndex() == 4 && follower.lastIndex() == 4;
                assert follower.lastTerm() == 9;
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
