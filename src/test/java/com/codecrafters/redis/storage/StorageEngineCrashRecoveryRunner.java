package com.codecrafters.redis.storage;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

public class StorageEngineCrashRecoveryRunner {
    private static final Path DIR =
            Path.of("test-data", "crash-audit-runner");

    public static void main(String[] args) throws Exception {
        if (args.length > 0) {
            if ("setup-flush".equals(args[0])) {
                deleteDirectory(DIR);
                Files.createDirectories(DIR);

                try (StorageEngine engine = new StorageEngine(DIR)) {
                    engine.put("flush-key", "flush-value");
                }

                System.out.println("FLUSH-CRASH SETUP PASSED");
                return;
            }

            if ("crash-flush".equals(args[0])) {
                try (StorageEngine engine = new StorageEngine(DIR)) {
                    engine.flush();
                }
                throw new AssertionError(
                        "Expected flush checkpoint was not reached."
                );
            }

            if ("setup-delete".equals(args[0])) {
                deleteDirectory(DIR);
                Files.createDirectories(DIR);

                try (StorageEngine engine = new StorageEngine(DIR)) {
                    engine.put("key-one", "value-one");
                    engine.flush();
                    engine.put("key-two", "value-two");
                    engine.flush();
                }

                System.out.println("DELETE-CRASH SETUP PASSED");
                return;
            }

            if ("crash".equals(args[0])) {
                try (StorageEngine engine = new StorageEngine(DIR)) {
                    engine.compact();
                }
                throw new AssertionError(
                        "Expected forced crash checkpoint was not reached."
                );
            }

            if ("crash-delete".equals(args[0])) {
                try (StorageEngine engine = new StorageEngine(DIR)) {
                    engine.compact();
                }
                throw new AssertionError(
                        "Expected old-table deletion checkpoint was not reached."
                );
            }

            if ("recover-flush".equals(args[0])) {
                try (StorageEngine engine = new StorageEngine(DIR)) {
                    var snapshot = engine.snapshot();

                    if (!"flush-value".equals(snapshot.get("flush-key"))) {
                        throw new AssertionError(
                                "Flush recovery mismatch: " + snapshot
                        );
                    }

                    System.out.println("FLUSH RECOVERY PASSED: " + snapshot);
                }
                return;
            }

            if ("recover".equals(args[0])) {
                try (StorageEngine engine = new StorageEngine(DIR)) {
                    var snapshot = engine.snapshot();

                    if (!"value-one".equals(snapshot.get("key-one"))
                            || !"value-two".equals(snapshot.get("key-two"))) {
                        throw new AssertionError(
                                "Recovered data mismatch: " + snapshot
                        );
                    }

                    System.out.println("RECOVERY PASSED: " + snapshot);
                }
                return;
            }

            throw new IllegalArgumentException("Unknown mode: " + args[0]);
        }

        deleteDirectory(DIR);
        Files.createDirectories(DIR);

        try (StorageEngine engine = new StorageEngine(DIR)) {
            engine.put("key-one", "value-one");
            engine.flush();
            engine.put("key-two", "value-two");
            engine.flush();
        }

        System.out.println("SETUP PASSED: two SSTables created.");

        String javaExe = Path.of(
                System.getProperty("java.home"), "bin", "java.exe"
        ).toString();

        String normalOut = Path.of("out").toAbsolutePath().toString();
        String instrumentedOut = Path.of(
                "test-data", "crash-audit-instrumented-out"
        ).toAbsolutePath().toString();

        String instrumentedClasspath =
                instrumentedOut + File.pathSeparator + normalOut;

        Process crashProcess = new ProcessBuilder(
                javaExe,
                "-Dcrash.audit.halt.after.manifest=true",
                "-cp", instrumentedClasspath,
                StorageEngineCrashRecoveryRunner.class.getName(),
                "crash"
        ).inheritIO().start();

        int crashExit = crashProcess.waitFor();

        if (crashExit != 73) {
            throw new AssertionError(
                    "Expected crash exit code 73, got " + crashExit
            );
        }

        System.out.println(
                "CRASH CHECKPOINT PASSED: child JVM exited with code 73."
        );

        Process recoveryProcess = new ProcessBuilder(
                javaExe,
                "-cp", normalOut,
                StorageEngineCrashRecoveryRunner.class.getName(),
                "recover"
        ).inheritIO().start();

        int recoveryExit = recoveryProcess.waitFor();

        if (recoveryExit != 0) {
            throw new AssertionError(
                    "Recovery process failed with exit code " + recoveryExit
            );
        }

        System.out.println(
                "CRASH-RECOVERY AUDIT PASSED for this checkpoint."
        );
    }

    private static void deleteDirectory(Path dir) throws Exception {
        if (!Files.exists(dir)) {
            return;
        }

        try (var paths = Files.walk(dir)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }
}