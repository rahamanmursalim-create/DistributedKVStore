package com.codecrafters.redis.storage;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

public class StorageEngineWalRecoveryTest {

    public static void main(String[] args) throws Exception {
        Path dir = Files.createTempDirectory("storage-engine-wal-recovery-");

        // Write data without flushing the MemTable.
        StorageEngine first = new StorageEngine(dir);
        first.put("name", "Mursalim");
        first.put("language", "Java");
        first.close();

        // Restart: unflushed data must be recovered from the WAL.
        StorageEngine second = new StorageEngine(dir);

        check("Mursalim".equals(second.get("name")),
                "Unflushed PUT recovery failed");
        check("Java".equals(second.get("language")),
                "Second unflushed PUT recovery failed");

        System.out.println("TEST 1 PASSED: Unflushed PUT recovery");

        // Delete must also survive a restart without a flush.
        second.delete("name");
        second.close();

        StorageEngine third = new StorageEngine(dir);

        check(third.get("name") == null,
                "Deleted key reappeared after WAL recovery");
        check("Java".equals(third.get("language")),
                "Unrelated key was lost after WAL recovery");

        System.out.println("TEST 2 PASSED: Unflushed DELETE recovery");

        third.close();

        // An expired WAL value must not reappear on restart, including when
        // an older SSTable contains a value for the same key.
        Path expiryDir = dir.resolve("expiry");
        StorageEngine expiryWriter = new StorageEngine(expiryDir);
        expiryWriter.put("expired", "old");
        expiryWriter.flush();
        expiryWriter.close();
        try (WriteAheadLog expiryWal = new WriteAheadLog(
                expiryDir.resolve("memtable-00000000000000000002.wal"))) {
            expiryWal.appendPut("expired", "short-lived",
                    System.currentTimeMillis() - 1);
        }

        StorageEngine expiryReader = new StorageEngine(expiryDir);
        check(expiryReader.get("expired") == null,
                "Expired WAL value resurrected an older SSTable value");
        expiryReader.close();

        System.out.println("ALL STORAGE ENGINE WAL RECOVERY TESTS PASSED");
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    @Test
    void runsExistingScenarioUnderJUnit() throws Exception {
        main(new String[0]);
    }
}
