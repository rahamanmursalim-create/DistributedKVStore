package com.codecrafters.redis.storage;

import java.util.List;

public class BloomFilterTest {
    public static void main(String[] args) {
        List<String> keys = List.of(
                "alpha", "beta", "gamma", "delta", "epsilon"
        );

        BloomFilter filter = new BloomFilter(keys);

        for (String key : keys) {
            if (!filter.mightContain(key)) {
                throw new AssertionError(
                        "False negative for existing key: " + key);
            }
        }

        System.out.println("PASS: All existing keys are detected");

        if (filter.mightContain(null)) {
            throw new AssertionError("Null key should not be present");
        }

        System.out.println("Bloom Filter tests passed");
    }
}
