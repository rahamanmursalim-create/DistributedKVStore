package com.codecrafters.redis.storage;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class StorageEngineConcurrencyTest {

    public static void main(String[] args) throws Exception {
        Path testDir = Path.of("test-data", "concurrency");

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
        int keysPerWorker = 100;
        ExecutorService pool = Executors.newFixedThreadPool(workers);
        CountDownLatch start = new CountDownLatch(1);

        try (StorageEngine engine = new StorageEngine(testDir)) {
            @SuppressWarnings("unchecked")
            Future<?>[] tasks = new Future<?>[workers];

            for (int worker = 0; worker < workers; worker++) {
                final int workerId = worker;

                tasks[worker] = pool.submit(() -> {
                    start.await();

                    for (int i = 0; i < keysPerWorker; i++) {
                        String key = "key-" + workerId + "-" + i;
                        String value = "value-" + workerId + "-" + i;

                        engine.put(key, value);

                        if (!value.equals(engine.get(key))) {
                            throw new AssertionError(
                                    "Concurrent read mismatch: " + key);
                        }

                        if (i % 25 == 0) {
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

            int expectedKeys = workers * keysPerWorker;

            if (engine.snapshot().size() != expectedKeys) {
                throw new AssertionError(
                        "Expected " + expectedKeys + " keys but got "
                                + engine.snapshot().size());
            }

            for (int worker = 0; worker < workers; worker++) {
                for (int i = 0; i < keysPerWorker; i++) {
                    String key = "key-" + worker + "-" + i;
                    String expected = "value-" + worker + "-" + i;

                    if (!expected.equals(engine.get(key))) {
                        throw new AssertionError(
                                "Missing or incorrect value: " + key);
                    }
                }
            }

            System.out.println(
                    "TEST PASSED: 8 concurrent workers, 800 keys, "
                            + "parallel writes/reads and flushes");
        } finally {
            pool.shutdownNow();
        }
    }
}
