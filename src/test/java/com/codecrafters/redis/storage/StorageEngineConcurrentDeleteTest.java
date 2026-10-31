package com.codecrafters.redis.storage;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class StorageEngineConcurrentDeleteTest {

    public static void main(String[] args) throws Exception {
        Path testDir = Path.of("test-data", "concurrent-delete");

        if (Files.exists(testDir)) {
            try (var paths = Files.walk(testDir)) {
                paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }

        int workers = 8;
        int keysPerWorker = 50;
        ExecutorService pool = Executors.newFixedThreadPool(workers);
        CountDownLatch start = new CountDownLatch(1);

        try {
            // Seed keys that workers will delete.
            try (StorageEngine engine = new StorageEngine(testDir)) {
                for (int w = 0; w < workers; w++) {
                    for (int i = 0; i < keysPerWorker; i++) {
                        engine.put("delete-" + w + "-" + i, "value-" + w + "-" + i);
                    }
                }
                engine.flush();
            }

            // Concurrently delete unique keys and flush.
            try (StorageEngine engine = new StorageEngine(testDir)) {
                @SuppressWarnings("unchecked")
                Future<?>[] tasks = new Future<?>[workers];

                for (int w = 0; w < workers; w++) {
                    final int workerId = w;

                    tasks[w] = pool.submit(() -> {
                        start.await();

                        for (int i = 0; i < keysPerWorker; i++) {
                            String key = "delete-" + workerId + "-" + i;

                            if (!engine.delete(key)) {
                                throw new AssertionError(
                                        "Expected key to be deleted: " + key);
                            }

                            if (i % 10 == 0) {
                                engine.flush();
                            }
                        }
                        return null;
                    });
                }

                start.countDown();

                for (Future<?> task : tasks) {
                    task.get(60, TimeUnit.SECONDS);
                }

                engine.flush();

                if (!engine.snapshot().isEmpty()) {
                    throw new AssertionError(
                            "Deleted keys remain in snapshot: "
                                    + engine.snapshot().size());
                }
            }

            // Restart and ensure deleted keys do not reappear.
            try (StorageEngine recovered = new StorageEngine(testDir)) {
                if (!recovered.snapshot().isEmpty()) {
                    throw new AssertionError(
                            "Deleted keys reappeared after restart: "
                                    + recovered.snapshot().size());
                }

                for (int w = 0; w < workers; w++) {
                    for (int i = 0; i < keysPerWorker; i++) {
                        String key = "delete-" + w + "-" + i;
                        if (recovered.get(key) != null) {
                            throw new AssertionError(
                                    "Deleted key recovered: " + key);
                        }
                    }
                }
            }

            System.out.println(
                    "TEST PASSED: 400 concurrent deletes, flushes, "
                            + "and restart recovery");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void runsExistingScenarioUnderJUnit() throws Exception {
        main(new String[0]);
    }
}