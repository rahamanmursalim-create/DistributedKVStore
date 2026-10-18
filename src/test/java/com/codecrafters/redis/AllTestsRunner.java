package com.codecrafters.redis;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/**
 * Platform-independent test runner for Maven.
 * Discovers and executes all *Test classes with JVM assertions (-ea) enabled,
 * returning a non-zero exit code if any test fails.
 */
public class AllTestsRunner {

    public static void main(String[] args) throws Exception {
        System.out.println("=================================================");
        System.out.println("     DistributedKVStore Test Suite Runner        ");
        System.out.println("=================================================");

        String javaHome = System.getProperty("java.home");
        String javaBin = Path.of(javaHome, "bin",
                System.getProperty("os.name").toLowerCase().contains("win") ? "java.exe" : "java").toString();
        Path testClassesPath = Path.of("target", "test-classes").toAbsolutePath();
        Path classesPath = Path.of("target", "classes").toAbsolutePath();
        String classpath = testClassesPath + File.pathSeparator + classesPath + File.pathSeparator + System.getProperty("java.class.path");

        List<String> testClasses = discoverTestClasses();
        if (testClasses.isEmpty()) {
            System.err.println("No test classes found to execute!");
            System.exit(1);
        }

        int passed = 0;
        int failed = 0;
        List<String> failedNames = new ArrayList<>();

        for (String className : testClasses) {
            System.out.println("\n>> RUNNING: " + className);
            long start = System.currentTimeMillis();

            ProcessBuilder pb = new ProcessBuilder(javaBin, "-ea", "-cp", classpath, className);
            pb.redirectErrorStream(true);
            Process proc = pb.start();

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println("   " + line);
                }
            }

            int exitCode = proc.waitFor();
            long duration = System.currentTimeMillis() - start;

            if (exitCode == 0) {
                passed++;
                System.out.println("   [PASS] " + className + " (" + duration + "ms)");
            } else {
                failed++;
                failedNames.add(className);
                System.err.println("   [FAIL] " + className + " (exit code " + exitCode + ")");
            }
        }

        System.out.println("\n=================================================");
        System.out.println("                 TEST SUMMARY                    ");
        System.out.println("=================================================");
        System.out.println("Total:  " + testClasses.size());
        System.out.println("Passed: " + passed);
        System.out.println("Failed: " + failed);

        if (failed > 0) {
            System.err.println("Failed classes: " + failedNames);
            System.exit(1);
        }
    }

    private static List<String> discoverTestClasses() {
        List<String> result = new ArrayList<>();
        Path testClassesDir = Path.of("target", "test-classes");

        if (Files.exists(testClassesDir)) {
            try (Stream<Path> stream = Files.walk(testClassesDir)) {
                stream.filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().endsWith("Test.class"))
                        .filter(p -> !p.getFileName().toString().contains("$"))
                        .forEach(p -> {
                            Path relative = testClassesDir.relativize(p);
                            String classPath = relative.toString().replace(File.separatorChar, '.');
                            String className = classPath.substring(0, classPath.length() - ".class".length());
                            result.add(className);
                        });
            } catch (Exception e) {
                System.err.println("Error discovering test classes: " + e.getMessage());
            }
        }

        if (result.isEmpty()) {
            return List.of(
                    "com.codecrafters.redis.RedisServerTest",
                    "com.codecrafters.redis.storage.BloomFilterTest",
                    "com.codecrafters.redis.storage.RedisStoreRecoveryTest",
                    "com.codecrafters.redis.storage.SSTableManifestTest",
                    "com.codecrafters.redis.storage.SSTableTest",
                    "com.codecrafters.redis.storage.StorageEngineCompactionTest",
                    "com.codecrafters.redis.storage.StorageEngineCompactionWalTest",
                    "com.codecrafters.redis.storage.StorageEngineConcurrencyTest",
                    "com.codecrafters.redis.storage.StorageEngineConcurrentDeleteTest",
                    "com.codecrafters.redis.storage.StorageEngineOrphanSSTableTest",
                    "com.codecrafters.redis.storage.StorageEngineTest",
                    "com.codecrafters.redis.storage.StorageEngineWalRecoveryTest",
                    "com.codecrafters.redis.storage.WriteAheadLogTest",
                    "com.codecrafters.redis.web.WebServerIntegrationTest"
            );
        }

        Collections.sort(result);
        return result;
    }
}
