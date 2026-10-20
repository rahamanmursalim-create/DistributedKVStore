package com.codecrafters.redis;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
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
        String classpath = buildForkClasspath(testClassesPath, classesPath);

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

    private static String buildForkClasspath(Path testClassesPath, Path classesPath) {
        LinkedHashSet<String> entries = new LinkedHashSet<>();
        entries.add(testClassesPath.toString());
        entries.add(classesPath.toString());
        Collections.addAll(entries, System.getProperty("java.class.path").split(java.util.regex.Pattern.quote(File.pathSeparator)));

        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        while (loader != null) {
            if (loader instanceof URLClassLoader urlLoader) {
                for (URL url : urlLoader.getURLs()) {
                    if ("file".equalsIgnoreCase(url.getProtocol())) {
                        try { entries.add(Path.of(url.toURI()).toString()); }
                        catch (Exception ignored) { }
                    }
                }
            }
            loader = loader.getParent();
        }
        // Maven's exec plugin can load project dependencies through a ClassRealm
        // without exposing their URLs as java.class.path entries.
        URL rocksDbClass = Thread.currentThread().getContextClassLoader()
                .getResource("org/rocksdb/RocksDBException.class");
        if (rocksDbClass != null) {
            try {
                if (rocksDbClass.openConnection() instanceof JarURLConnection jarConnection) {
                    URL jar = jarConnection.getJarFileURL();
                    if ("file".equalsIgnoreCase(jar.getProtocol())) entries.add(Path.of(jar.toURI()).toString());
                }
            } catch (Exception ignored) { }
        }
        return String.join(File.pathSeparator, entries);
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
