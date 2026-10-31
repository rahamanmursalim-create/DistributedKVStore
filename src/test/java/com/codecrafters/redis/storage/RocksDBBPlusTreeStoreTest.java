package com.codecrafters.redis.storage;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

public final class RocksDBBPlusTreeStoreTest {
    public static void main(String[] args) throws Exception {
        Path directory = Files.createTempDirectory("rocksdb-btree-test");
        Map<String, String> entries = new LinkedHashMap<>();
        try (RocksDBBPlusTreeStore store = new RocksDBBPlusTreeStore(directory)) {
            for (int i = 0; i < 1_000; i++) entries.put("key-" + i, "value-" + i);
            store.putBatch(entries);
            assert entries.size() == store.snapshot().size();
            assert "value-578".equals(store.get("key-578"));
            assert store.delete("key-578");
            assert !store.delete("missing");
        }
        try (RocksDBBPlusTreeStore recovered = new RocksDBBPlusTreeStore(directory)) {
            assert recovered.get("key-578") == null;
            assert "value-999".equals(recovered.get("key-999"));
            assert recovered.snapshot().size() == 999;
        }
        System.out.println("RocksDBBPlusTreeStoreTest passed");
    }

    @Test
    void runsExistingScenarioUnderJUnit() throws Exception {
        main(new String[0]);
    }
}
