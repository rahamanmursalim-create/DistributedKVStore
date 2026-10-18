
package com.codecrafters.redis.storage;

import com.codecrafters.redis.command.Command;
import com.codecrafters.redis.command.CommandHandler;
import com.codecrafters.redis.protocol.RespValue;

import java.nio.file.Files;
import java.nio.file.Path;

public class StorageEngineCommandTest {

    public static void main(String[] args) throws Exception {
        Path directory = Files.createTempDirectory("kv-command-test-");

        try {
            testCommandsAndRestart(directory);

            System.out.println("ALL STORAGE ENGINE COMMAND TESTS PASSED");
        } finally {
            try (var paths = Files.walk(directory)) {
                paths.sorted(java.util.Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                Files.deleteIfExists(path);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
            }
        }
    }

    private static void testCommandsAndRestart(Path directory)
            throws Exception {

        try (StorageEngine engine = new StorageEngine(directory)) {
            CommandHandler handler = new CommandHandler(engine);

            assertResponse("+OK", handler.execute(
                    command("SET", "user:1", "Alice")).toString(),
                    "SET");

            assertResponse("+OK", handler.execute(
                    command("SET", "user:2", "Bob")).toString(),
                    "SET second key");

            assertResponse(":1", handler.execute(
                    command("EXISTS", "user:1")).toString(),
                    "EXISTS existing key");

            assertResponse(":0", handler.execute(
                    command("EXISTS", "missing")).toString(),
                    "EXISTS missing key");

            RespValue keys = handler.execute(command("KEYS", "user:*"));
            String keysResponse = keys.toString();

            if (!keysResponse.contains("user:1")
                    || !keysResponse.contains("user:2")) {
                throw new AssertionError(
                        "KEYS user:* did not return both matching keys: "
                                + keysResponse);
            }

            assertResponse(":1", handler.execute(
                    command("DEL", "user:1")).toString(),
                    "DEL existing key");

            assertResponse(":0", handler.execute(
                    command("EXISTS", "user:1")).toString(),
                    "EXISTS after deletion");

            engine.flush();
        }

        try (StorageEngine recovered = new StorageEngine(directory)) {
            CommandHandler handler = new CommandHandler(recovered);

            assertResponse("$-1", handler.execute(
                    command("GET", "user:1")).toString(),
                    "Deleted key remains absent after restart");

            assertResponse(":1", handler.execute(
                    command("EXISTS", "user:2")).toString(),
                    "Remaining key survives restart");

            assertResponse(":0", handler.execute(
                    command("DEL", "user:1")).toString(),
                    "Deleting absent key returns zero");

            System.out.println("PASSED: SET and EXISTS");
            System.out.println("PASSED: KEYS wildcard matching");
            System.out.println("PASSED: DEL and EXISTS after deletion");
            System.out.println("PASSED: Deletion and values survive restart");
        }
    }

    private static Command command(String... args) {
        return new Command(args[0], java.util.Arrays.asList(args).subList(
                1, args.length));
    }

    private static void assertResponse(
            String expected, String actual, String testName) {
        if (!expected.equals(actual)) {
            throw new AssertionError(
                    testName + ": expected " + expected + ", got " + actual);
        }
    }
}
