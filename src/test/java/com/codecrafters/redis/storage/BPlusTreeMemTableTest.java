package com.codecrafters.redis.storage;

import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

public final class BPlusTreeMemTableTest {
    public static void main(String[] args) {
        MemTable table = new MemTable();
        TreeMap<String, String> reference = new TreeMap<>();
        Random random = new Random(0x5eed);
        for (int i = 0; i < 20_000; i++) {
            String key = String.format("k-%05d", random.nextInt(5_000));
            if (random.nextInt(5) == 0) {
                String actual = table.delete(key);
                String expected = reference.remove(key);
                assert java.util.Objects.equals(actual, expected);
                reference.put(key, null);
            } else {
                String value = "v-" + i;
                table.put(key, value);
                reference.put(key, value);
            }
            assert table.containsKey(key);
            assert java.util.Objects.equals(table.get(key), reference.get(key));
        }
        assert table.snapshot().equals(reference);
        assert table.size() == reference.size();
        for (Map.Entry<String, String> entry : reference.entrySet()) {
            assert java.util.Objects.equals(table.get(entry.getKey()), entry.getValue());
        }
        table.clear();
        assert table.isEmpty();
        assert table.snapshot().isEmpty();
        BPlusTree<Integer> churn = new BPlusTree<>();
        for (int round = 0; round < 8; round++) {
            for (int i = 0; i < 2_000; i++) churn.put("round-" + round + "-key-" + i, i);
            for (int i = 0; i < 2_000; i++) assert churn.remove("round-" + round + "-key-" + i) == i;
            assert churn.size() == 0;
            assert churn.snapshot().isEmpty();
        }
        System.out.println("BPlusTreeMemTableTest passed");
    }
}
