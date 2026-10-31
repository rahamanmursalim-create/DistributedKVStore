package com.codecrafters.redis.raft;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class RaftLogTest {

    public static void main(String[] args) {
        testRaftLogEntryBasics();
        testRaftLogEntryValidation();
        testRaftLogEmpty();
        testRaftLogAppendAndGet();
        testRaftLogEntriesFrom();
        testRaftLogTruncate();

        System.out.println("ALL RAFT LOG TESTS PASSED");
    }

    private static void testRaftLogEntryBasics() {
        byte[] cmd = "SET foo bar".getBytes(StandardCharsets.UTF_8);
        RaftLogEntry entry1 = new RaftLogEntry(1, 2, cmd);
        RaftLogEntry entry2 = new RaftLogEntry(1, 2, cmd);

        assert entry1.getIndex() == 1 : "Expected index 1";
        assert entry1.getTerm() == 2 : "Expected term 2";
        assert Arrays.equals(entry1.getCommand(), cmd) : "Command content mismatch";
        assert entry1.equals(entry2) : "Entries should be equal";
        assert entry1.hashCode() == entry2.hashCode() : "HashCodes should match";

        // Defensive copy check
        cmd[0] = 'X';
        assert !Arrays.equals(entry1.getCommand(), cmd) : "RaftLogEntry should defensively copy command";
        System.out.println("TEST 1 PASSED: RaftLogEntry basics and defensive copy");
    }

    private static void testRaftLogEntryValidation() {
        try {
            new RaftLogEntry(0, 1, new byte[0]);
            throw new AssertionError("Should reject non-positive index");
        } catch (IllegalArgumentException expected) {
        }

        try {
            new RaftLogEntry(1, -1, new byte[0]);
            throw new AssertionError("Should reject negative term");
        } catch (IllegalArgumentException expected) {
        }
        System.out.println("TEST 2 PASSED: RaftLogEntry parameter validation");
    }

    private static void testRaftLogEmpty() {
        RaftLog log = new RaftLog();
        assert log.size() == 0 : "Expected empty log";
        assert log.lastIndex() == 0 : "Expected last index 0";
        assert log.lastTerm() == 0 : "Expected last term 0";
        assert log.get(0).isEmpty() : "Index 0 should be empty";
        assert log.get(1).isEmpty() : "Index 1 should be empty";
        System.out.println("TEST 3 PASSED: RaftLog empty state");
    }

    private static void testRaftLogAppendAndGet() {
        RaftLog log = new RaftLog();
        byte[] c1 = "CMD1".getBytes(StandardCharsets.UTF_8);
        byte[] c2 = "CMD2".getBytes(StandardCharsets.UTF_8);

        RaftLogEntry e1 = log.append(1, c1);
        assert e1.getIndex() == 1 : "Expected index 1";
        assert e1.getTerm() == 1 : "Expected term 1";
        assert log.size() == 1 : "Expected size 1";
        assert log.lastIndex() == 1 : "Expected lastIndex 1";
        assert log.lastTerm() == 1 : "Expected lastTerm 1";

        RaftLogEntry e2 = log.append(2, c2);
        assert e2.getIndex() == 2 : "Expected index 2";
        assert e2.getTerm() == 2 : "Expected term 2";
        assert log.size() == 2 : "Expected size 2";
        assert log.lastIndex() == 2 : "Expected lastIndex 2";
        assert log.lastTerm() == 2 : "Expected lastTerm 2";

        Optional<RaftLogEntry> fetched = log.get(1);
        assert fetched.isPresent() && fetched.get().equals(e1) : "Failed to fetch entry 1";

        Optional<RaftLogEntry> fetched2 = log.get(2);
        assert fetched2.isPresent() && fetched2.get().equals(e2) : "Failed to fetch entry 2";

        assert log.get(3).isEmpty() : "Index 3 should be empty";
        System.out.println("TEST 4 PASSED: RaftLog append and get");
    }

    private static void testRaftLogEntriesFrom() {
        RaftLog log = new RaftLog();
        log.append(1, "A".getBytes(StandardCharsets.UTF_8));
        log.append(1, "B".getBytes(StandardCharsets.UTF_8));
        log.append(2, "C".getBytes(StandardCharsets.UTF_8));

        List<RaftLogEntry> from1 = log.entriesFrom(1);
        assert from1.size() == 3 : "Expected 3 entries";

        List<RaftLogEntry> from2 = log.entriesFrom(2);
        assert from2.size() == 2 : "Expected 2 entries";
        assert from2.get(0).getIndex() == 2;
        assert from2.get(1).getIndex() == 3;

        List<RaftLogEntry> fromEnd = log.entriesFrom(4);
        assert fromEnd.isEmpty() : "Expected empty list from end";

        try {
            log.entriesFrom(0);
            throw new AssertionError("Should reject index 0");
        } catch (IllegalArgumentException expected) {
        }

        try {
            log.entriesFrom(5);
            throw new AssertionError("Should reject index > size + 1");
        } catch (IllegalArgumentException expected) {
        }
        System.out.println("TEST 5 PASSED: RaftLog entriesFrom slice validation");
    }

    private static void testRaftLogTruncate() {
        RaftLog log = new RaftLog();
        log.append(1, "A".getBytes(StandardCharsets.UTF_8));
        log.append(1, "B".getBytes(StandardCharsets.UTF_8));
        log.append(2, "C".getBytes(StandardCharsets.UTF_8));

        log.truncateFrom(3);
        assert log.size() == 2 : "Expected size 2 after truncate from 3";
        assert log.lastIndex() == 2;
        assert log.lastTerm() == 1;

        log.truncateFrom(1);
        assert log.size() == 0 : "Expected size 0 after truncate from 1";
        assert log.lastIndex() == 0;
        assert log.lastTerm() == 0;
        System.out.println("TEST 6 PASSED: RaftLog truncateFrom");
    }

    @Test
    void runsExistingScenarioUnderJUnit() throws Exception {
        main(new String[0]);
    }
}
