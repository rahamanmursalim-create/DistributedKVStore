
package com.codecrafters.redis.storage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;

public class WriteAheadLogTest {

    public static void main(String[] args) throws Exception {
        Path testDir = Path.of("test-data", "wal-tests");

        if (Files.exists(testDir)) {
            try (var paths = Files.walk(testDir)) {
                paths.sorted(Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
            }
        }

        Files.createDirectories(testDir);
        Path walPath = testDir.resolve("wal.log");

        // TEST 1: PUT and DELETE replay.
        try (WriteAheadLog wal = new WriteAheadLog(walPath)) {
            wal.appendPut("name", "Mursalim", -1L);
            wal.appendPut("language", "Java", -1L);
            wal.appendDelete("name");

            List<WriteAheadLog.Record> records = wal.replay();

            check(records.size() == 3, "Expected 3 WAL records");
            check(records.get(0).getOperation()
                    == WriteAheadLog.Operation.PUT, "First record must be PUT");
            check("Mursalim".equals(records.get(0).getValue()),
                    "PUT value mismatch");
            check(records.get(2).getOperation()
                    == WriteAheadLog.Operation.DELETE,
                    "Last record must be DELETE");

            System.out.println("TEST 1 PASSED: PUT and DELETE replay");
        }

        // TEST 2: Records survive closing and reopening.
        try (WriteAheadLog wal = new WriteAheadLog(walPath)) {
            List<WriteAheadLog.Record> records = wal.replay();

            check(records.size() == 3, "Records lost after reopening");
            check("language".equals(records.get(1).getKey()),
                    "Recovered key mismatch");

            System.out.println("TEST 2 PASSED: WAL restart recovery");
        }

        // TEST 3: Incomplete final record is discarded.
        Files.writeString(
                walPath,
                "PUT\tpartial",
                StandardCharsets.UTF_8,
                StandardOpenOption.APPEND
        );

        try (WriteAheadLog wal = new WriteAheadLog(walPath)) {
            List<WriteAheadLog.Record> records = wal.replay();

            check(records.size() == 3,
                    "Incomplete final record should be ignored");

            wal.appendPut("afterCrash", "recovered", -1L);

            List<WriteAheadLog.Record> updated = wal.replay();
            check(updated.size() == 4,
                    "New record failed after partial-tail recovery");
            check("afterCrash".equals(updated.get(3).getKey()),
                    "Record after recovery mismatch");

            System.out.println("TEST 3 PASSED: Incomplete tail recovery");
        }

        // TEST 3B: Recovered WAL remains valid after another restart.
        try (WriteAheadLog wal = new WriteAheadLog(walPath)) {
            List<WriteAheadLog.Record> records = wal.replay();

            check(records.size() == 4,
                    "Records lost after recovery restart");
            check("afterCrash".equals(records.get(3).getKey()),
                    "Recovered append missing after restart");

            System.out.println(
                    "TEST 3B PASSED: Recovery survives another restart");
        }

        // TEST 4: Corrupted checksummed record must be rejected.
        try (WriteAheadLog wal = new WriteAheadLog(walPath)) {
            wal.appendPut("checksumKey", "checksumValue", -1L);
        }

        byte[] corrupted = Files.readAllBytes(walPath);
        String encodedValue = java.util.Base64.getUrlEncoder().withoutPadding()
                .encodeToString("checksumValue".getBytes(StandardCharsets.UTF_8));
        byte[] needle = encodedValue.getBytes(StandardCharsets.UTF_8);

        int position = -1;
        outer:
        for (int i = corrupted.length - needle.length; i >= 0; i--) {
            for (int j = 0; j < needle.length; j++) {
                if (corrupted[i + j] != needle[j]) {
                    continue outer;
                }
            }
            position = i;
            break;
        }

        check(position >= 0, "Could not locate test value in WAL");
        corrupted[position] = (byte) (corrupted[position] == 'A' ? 'B' : 'A');
        Files.write(walPath, corrupted);

        boolean checksumRejected = false;
        try (WriteAheadLog wal = new WriteAheadLog(walPath)) {
            wal.replay();
        } catch (IOException expected) {
            checksumRejected = expected.getMessage().contains("checksum mismatch");
        }

        check(checksumRejected, "Corrupted WAL record was not rejected");
        System.out.println("TEST 4 PASSED: Checksum corruption detected");
        // TEST 5: Legacy WAL records remain readable.
        Path legacyPath = testDir.resolve("legacy.log");
        String legacyPut = "PUT\t"
                + java.util.Base64.getUrlEncoder().withoutPadding()
                    .encodeToString("oldKey".getBytes(StandardCharsets.UTF_8))
                + "\t"
                + java.util.Base64.getUrlEncoder().withoutPadding()
                    .encodeToString("oldValue".getBytes(StandardCharsets.UTF_8))
                + "\t-1\n";
        String legacyDelete = "DEL\t"
                + java.util.Base64.getUrlEncoder().withoutPadding()
                    .encodeToString("deletedKey".getBytes(StandardCharsets.UTF_8))
                + "\n";
        Files.writeString(legacyPath, legacyPut + legacyDelete, StandardCharsets.UTF_8);

        try (WriteAheadLog wal = new WriteAheadLog(legacyPath)) {
            List<WriteAheadLog.Record> records = wal.replay();
            check(records.size() == 2, "Legacy records were not recovered");
            check("oldKey".equals(records.get(0).getKey()), "Legacy PUT key mismatch");
            check("oldValue".equals(records.get(0).getValue()), "Legacy PUT value mismatch");
            check(records.get(1).getOperation() == WriteAheadLog.Operation.DELETE,
                    "Legacy DEL operation mismatch");
            check("deletedKey".equals(records.get(1).getKey()), "Legacy DEL key mismatch");
            System.out.println("TEST 5 PASSED: Legacy WAL compatibility");
        }

        // TEST 6: Operations after close must fail clearly.
        WriteAheadLog closedWal = new WriteAheadLog(
                testDir.resolve("closed.log"));
        closedWal.close();

        boolean appendRejected = false;
        try {
            closedWal.appendPut("key", "value", -1L);
        } catch (IOException expected) {
            appendRejected = expected.getMessage().contains("closed");
        }
        check(appendRejected, "Append after close must be rejected");

        boolean replayRejected = false;
        try {
            closedWal.replay();
        } catch (IOException expected) {
            replayRejected = expected.getMessage().contains("closed");
        }
        check(replayRejected, "Replay after close must be rejected");

        closedWal.close();
        System.out.println("TEST 6 PASSED: Closed WAL rejects operations");

        System.out.println("ALL WAL TESTS PASSED");
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
