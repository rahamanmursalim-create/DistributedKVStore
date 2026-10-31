package com.codecrafters.redis.storage;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class SSTableManifestTest {
    public static void main(String[] args) throws Exception {
        Path dir = Path.of("test-data", "manifest-tests");
        if (Files.exists(dir)) {
            try (var paths = Files.walk(dir)) {
                paths.sorted(java.util.Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
            }
        }
        Files.createDirectories(dir);

        // TEST 1: Save and load IDs.
        SSTableManifest manifest = new SSTableManifest(dir);
        manifest.save(List.of(1L, 2L, 5L));
        check(manifest.load().equals(List.of(1L, 2L, 5L)),
                "Manifest round-trip failed");
        System.out.println("TEST 1 PASSED: Manifest round-trip");

        // TEST 2: Legacy plain-text manifest remains readable.
        Files.writeString(dir.resolve("MANIFEST"), "1\n2\n",
                StandardCharsets.UTF_8);
        check(manifest.load().equals(List.of(1L, 2L)),
                "Legacy manifest compatibility failed");
        System.out.println("TEST 2 PASSED: Legacy manifest compatibility");

        // TEST 3: Invalid IDs are rejected.
        Files.writeString(dir.resolve("MANIFEST"), "1\n0\n",
                StandardCharsets.UTF_8);
        boolean rejected = false;
        try {
            manifest.load();
        } catch (IOException expected) {
            rejected = true;
        }
        check(rejected, "Invalid manifest ID was accepted");
        System.out.println("TEST 3 PASSED: Invalid ID rejected");

        // TEST 4: Corrupted manifest entry is rejected.
        Files.writeString(dir.resolve("MANIFEST"), "1\nabc\n2\n",
                StandardCharsets.UTF_8);
        rejected = false;
        try {
            manifest.load();
        } catch (IOException expected) {
            rejected = true;
        }
        check(rejected, "Corrupted manifest entry was accepted");
        System.out.println("TEST 4 PASSED: Corrupted entry rejected");

        // TEST 5: Saving an invalid ID must be rejected.
        boolean invalidSaveRejected = false;
        try {
            manifest.save(List.of(1L, 0L, 2L));
        } catch (IllegalArgumentException expected) {
            invalidSaveRejected = true;
        }
        check(invalidSaveRejected,
                "Saving an invalid SSTable ID should throw IllegalArgumentException");
        System.out.println("TEST 5 PASSED: Invalid save input rejected");

        // TEST 6: Invalid save must preserve the existing manifest.
        manifest.save(List.of(1L, 2L, 3L));
        byte[] originalManifest = Files.readAllBytes(dir.resolve("MANIFEST"));

        invalidSaveRejected = false;
        try {
            manifest.save(List.of(4L, 0L, 5L));
        } catch (IllegalArgumentException expected) {
            invalidSaveRejected = true;
        }

        check(invalidSaveRejected, "Invalid save was not rejected");
        check(java.util.Arrays.equals(originalManifest,
                        Files.readAllBytes(dir.resolve("MANIFEST"))),
                "Invalid save modified the existing manifest");
        System.out.println("TEST 6 PASSED: Invalid save preserves manifest");

        // TEST 7: Duplicate SSTable IDs must be rejected.
        boolean duplicateRejected = false;
        try {
            manifest.save(List.of(1L, 2L, 2L));
        } catch (IllegalArgumentException expected) {
            duplicateRejected = true;
        }
        check(duplicateRejected, "Duplicate SSTable IDs were accepted");
        System.out.println("TEST 7 PASSED: Duplicate IDs rejected");

        // TEST 8: Duplicate IDs in a loaded manifest must be rejected.
        Files.writeString(dir.resolve("MANIFEST"), "1\n2\n2\n",
                StandardCharsets.UTF_8);
        boolean duplicateLoadRejected = false;
        try {
            manifest.load();
        } catch (IOException expected) {
            duplicateLoadRejected = true;
        }
        check(duplicateLoadRejected,
                "Duplicate IDs in manifest file were accepted");
        System.out.println("TEST 8 PASSED: Duplicate IDs in loaded manifest rejected");

        System.out.println("ALL MANIFEST TESTS PASSED");
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
