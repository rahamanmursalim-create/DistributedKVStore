package com.codecrafters.redis.storage;

import java.nio.charset.StandardCharsets;
import java.util.Collection;

public final class BloomFilter {
    private final byte[] bits;
    private final int hashCount;

    public BloomFilter(Collection<String> keys) {
        this(Math.max(1, keys.size()));
        for (String key : keys) {
            add(key);
        }
    }

    public BloomFilter(int expectedKeys) {
        int count = Math.max(1, expectedKeys);
        int bitCount = Math.max(64, count * 10);
        bits = new byte[(bitCount + 7) / 8];
        hashCount = 7;
    }

    public void add(String key) {
        if (key != null) {
            update(key, true);
        }
    }

    public boolean mightContain(String key) {
        if (key == null) {
            return false;
        }
        long[] hashes = hashes(key);
        int bitCount = bits.length * 8;

        for (int i = 0; i < hashCount; i++) {
            int position = (int) Math.floorMod(
                    hashes[0] + i * hashes[1], bitCount);
            if ((bits[position / 8] & (1 << (position % 8))) == 0) {
                return false;
            }
        }
        return true;
    }

    private void update(String key, boolean set) {
        long[] hashes = hashes(key);
        int bitCount = bits.length * 8;

        for (int i = 0; i < hashCount; i++) {
            int position = (int) Math.floorMod(
                    hashes[0] + i * hashes[1], bitCount);
            bits[position / 8] |= (byte) (1 << (position % 8));
        }
    }

    private static long[] hashes(String key) {
        byte[] data = key.getBytes(StandardCharsets.UTF_8);
        long first = 0xcbf29ce484222325L;
        long second = 0x9e3779b97f4a7c15L;

        for (byte value : data) {
            first = (first ^ (value & 0xff)) * 0x100000001b3L;
            second ^= (value & 0xff) + (second << 6) + (second >>> 2);
        }

        second |= 1L;
        return new long[]{first, second};
    }
}

