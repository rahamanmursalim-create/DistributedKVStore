package com.codecrafters.redis.storage;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

public final class MVCCTransactionTest {
    public static void main(String[] args) throws Exception {
        Path directory = Files.createTempDirectory("mvcc-store-test");
        Path wal = directory.resolve("wal.log");
        try {
            try (RedisStore store = new RedisStore(wal, false)) {
                store.set("shared", "v1");
                MVCCTransaction reader = store.beginTransaction();
                assert reader.get("shared").equals("v1");
                store.set("shared", "v2");
                assert reader.get("shared").equals("v1") : "snapshot read changed after concurrent write";
                reader.put("independent", "ok");
                reader.commit();
                assert store.get("independent").equals("ok");

                MVCCTransaction staleWriter = store.beginTransaction();
                staleWriter.put("shared", "stale");
                store.set("shared", "v3");
                boolean conflicted = false;
                try { staleWriter.commit(); }
                catch (TransactionConflictException expected) { conflicted = true; }
                assert conflicted : "overlapping snapshot writes must conflict";

                store.set("remove", "old");
                try (MVCCTransaction transaction = store.beginTransaction()) {
                    transaction.put("left", "L");
                    transaction.put("right", "R");
                    assert transaction.delete("remove");
                    transaction.commit();
                }
                assert store.get("left").equals("L");
                assert store.get("right").equals("R");
                assert store.get("remove") == null;

                try (MVCCTransaction rollback = store.beginTransaction()) {
                    rollback.put("not-committed", "value");
                }
                assert store.get("not-committed") == null;
            }

            // A torn transaction record is discarded as a whole on recovery.
            Files.writeString(wal, "TXN2\tpartial", java.nio.file.StandardOpenOption.APPEND);
            try (RedisStore recovered = new RedisStore(wal, false)) {
                assert recovered.get("shared").equals("v3");
                assert recovered.get("left").equals("L");
                assert recovered.get("right").equals("R");
                assert recovered.get("remove") == null;
                assert recovered.get("not-committed") == null;
            }
            System.out.println("MVCCTransactionTest passed");
        } finally {
            try (var paths = Files.walk(directory)) {
                paths.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
                    try { Files.deleteIfExists(path); }
                    catch (Exception e) { throw new RuntimeException(e); }
                });
            }
        }
    }

    @Test
    void runsExistingScenarioUnderJUnit() throws Exception {
        main(new String[0]);
    }
}
