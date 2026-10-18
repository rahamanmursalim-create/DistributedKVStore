package com.codecrafters.redis.command;

import com.codecrafters.redis.protocol.RespValue;
import com.codecrafters.redis.storage.StorageEngine;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;

/**
 * Tests for CommandHandler using the storage engine backend.
 */
public final class CommandHandlerTest {

    private CommandHandlerTest() {
    }

    public static void main(String[] args) throws Exception {
        Path testDir = Files.createTempDirectory("command-handler-test-");

        try {
            testPing(testDir);
            testPingWithArg(testDir);
            testEcho(testDir);
            testEchoNoArg(testDir);
            testSetAndGet(testDir);
            testGetMissing(testDir);
            testDelExisting(testDir);
            testDelMissing(testDir);
            testSetWithPx(testDir);
            testSetWithEx(testDir);
            testUnknownCommand(testDir);
            testPutCommand(testDir);
            testExistsCommand(testDir);
            testInfoCommand(testDir);
            testEmptyCommand(testDir);
            testSetInvalidPx(testDir);
            testMalformedSetCommand(testDir);

            System.out.println("ALL COMMAND HANDLER TESTS PASSED");
        } finally {
            deleteDirectory(testDir);
        }
    }

    private static void testPing(Path root) throws Exception {
        Path dir = root.resolve("ping");
        try (StorageEngine engine = new StorageEngine(dir)) {
            CommandHandler handler = new CommandHandler(engine);
            RespValue response = handler.execute(
                    new Command("PING", List.of()));
            assertResp("+PONG", response, "PING");
        }
        System.out.println("TEST 1 PASSED: PING returns PONG");
    }

    private static void testPingWithArg(Path root) throws Exception {
        Path dir = root.resolve("ping-arg");
        try (StorageEngine engine = new StorageEngine(dir)) {
            CommandHandler handler = new CommandHandler(engine);
            RespValue response = handler.execute(
                    new Command("PING", List.of("hello")));
            assertResp("$hello", response, "PING with arg");
        }
        System.out.println("TEST 2 PASSED: PING with arg returns bulk string");
    }

    private static void testEcho(Path root) throws Exception {
        Path dir = root.resolve("echo");
        try (StorageEngine engine = new StorageEngine(dir)) {
            CommandHandler handler = new CommandHandler(engine);
            RespValue response = handler.execute(
                    new Command("ECHO", List.of("world")));
            assertResp("$world", response, "ECHO");
        }
        System.out.println("TEST 3 PASSED: ECHO returns arg");
    }

    private static void testEchoNoArg(Path root) throws Exception {
        Path dir = root.resolve("echo-noarg");
        try (StorageEngine engine = new StorageEngine(dir)) {
            CommandHandler handler = new CommandHandler(engine);
            RespValue response = handler.execute(
                    new Command("ECHO", List.of()));
            check(response.toString().startsWith("-"),
                    "ECHO without arg should return error");
        }
        System.out.println("TEST 4 PASSED: ECHO without arg returns error");
    }

    private static void testSetAndGet(Path root) throws Exception {
        Path dir = root.resolve("set-get");
        try (StorageEngine engine = new StorageEngine(dir)) {
            CommandHandler handler = new CommandHandler(engine);

            RespValue setResp = handler.execute(
                    new Command("SET", List.of("key", "value")));
            assertResp("+OK", setResp, "SET");

            RespValue getResp = handler.execute(
                    new Command("GET", List.of("key")));
            assertResp("$value", getResp, "GET existing");
        }
        System.out.println("TEST 5 PASSED: SET and GET");
    }

    private static void testGetMissing(Path root) throws Exception {
        Path dir = root.resolve("get-missing");
        try (StorageEngine engine = new StorageEngine(dir)) {
            CommandHandler handler = new CommandHandler(engine);
            RespValue response = handler.execute(
                    new Command("GET", List.of("nonexistent")));
            assertResp("$-1", response, "GET missing");
        }
        System.out.println("TEST 6 PASSED: GET missing returns null bulk");
    }

    private static void testDelExisting(Path root) throws Exception {
        Path dir = root.resolve("del-exists");
        try (StorageEngine engine = new StorageEngine(dir)) {
            CommandHandler handler = new CommandHandler(engine);
            handler.execute(new Command("SET", List.of("key", "value")));
            RespValue response = handler.execute(
                    new Command("DEL", List.of("key")));
            assertResp(":1", response, "DEL existing");
        }
        System.out.println("TEST 7 PASSED: DEL existing returns :1");
    }

    private static void testDelMissing(Path root) throws Exception {
        Path dir = root.resolve("del-missing");
        try (StorageEngine engine = new StorageEngine(dir)) {
            CommandHandler handler = new CommandHandler(engine);
            RespValue response = handler.execute(
                    new Command("DEL", List.of("nonexistent")));
            assertResp(":0", response, "DEL missing");
        }
        System.out.println("TEST 8 PASSED: DEL missing returns :0");
    }

    private static void testSetWithPx(Path root) throws Exception {
        Path dir = root.resolve("set-px");
        try (StorageEngine engine = new StorageEngine(dir)) {
            CommandHandler handler = new CommandHandler(engine);
            RespValue setResp = handler.execute(
                    new Command("SET", List.of("key", "value", "PX", "5000")));
            assertResp("+OK", setResp, "SET PX");
            RespValue getResp = handler.execute(
                    new Command("GET", List.of("key")));
            assertResp("$value", getResp, "GET before PX expiry");
        }
        System.out.println("TEST 9 PASSED: SET with PX");
    }

    private static void testSetWithEx(Path root) throws Exception {
        Path dir = root.resolve("set-ex");
        try (StorageEngine engine = new StorageEngine(dir)) {
            CommandHandler handler = new CommandHandler(engine);
            RespValue setResp = handler.execute(
                    new Command("SET", List.of("key", "value", "EX", "10")));
            assertResp("+OK", setResp, "SET EX");
            RespValue getResp = handler.execute(
                    new Command("GET", List.of("key")));
            assertResp("$value", getResp, "GET before EX expiry");
        }
        System.out.println("TEST 10 PASSED: SET with EX");
    }

    private static void testUnknownCommand(Path root) throws Exception {
        Path dir = root.resolve("unknown-cmd");
        try (StorageEngine engine = new StorageEngine(dir)) {
            CommandHandler handler = new CommandHandler(engine);
            RespValue response = handler.execute(
                    new Command("FOOBAR", List.of()));
            check(response.toString().startsWith("-"),
                    "Unknown command should return error");
        }
        System.out.println("TEST 11 PASSED: Unknown command returns error");
    }

    private static void testPutCommand(Path root) throws Exception {
        Path dir = root.resolve("put-cmd");
        try (StorageEngine engine = new StorageEngine(dir)) {
            CommandHandler handler = new CommandHandler(engine);
            RespValue putResp = handler.execute(
                    new Command("PUT", List.of("key", "value")));
            assertResp("+OK", putResp, "PUT");
            RespValue getResp = handler.execute(
                    new Command("GET", List.of("key")));
            assertResp("$value", getResp, "GET after PUT");
        }
        System.out.println("TEST 12 PASSED: PUT command");
    }

    private static void testExistsCommand(Path root) throws Exception {
        Path dir = root.resolve("exists-cmd");
        try (StorageEngine engine = new StorageEngine(dir)) {
            CommandHandler handler = new CommandHandler(engine);
            RespValue before = handler.execute(
                    new Command("EXISTS", List.of("key")));
            assertResp(":0", before, "EXISTS before SET");
            handler.execute(new Command("SET", List.of("key", "value")));
            RespValue after = handler.execute(
                    new Command("EXISTS", List.of("key")));
            assertResp(":1", after, "EXISTS after SET");
        }
        System.out.println("TEST 13 PASSED: EXISTS command");
    }

    private static void testInfoCommand(Path root) throws Exception {
        Path dir = root.resolve("info-cmd");
        try (StorageEngine engine = new StorageEngine(dir)) {
            CommandHandler handler = new CommandHandler(engine);
            RespValue response = handler.execute(
                    new Command("INFO", List.of()));
            check(response.toString().startsWith("$"),
                    "INFO should return bulk string");
        }
        System.out.println("TEST 14 PASSED: INFO command");
    }

    private static void testEmptyCommand(Path root) throws Exception {
        Path dir = root.resolve("empty-cmd");
        try (StorageEngine engine = new StorageEngine(dir)) {
            CommandHandler handler = new CommandHandler(engine);
            RespValue response = handler.execute(
                    new Command("", List.of()));
            check(response.toString().startsWith("-"),
                    "Empty command should return error");
        }
        System.out.println("TEST 15 PASSED: Empty command returns error");
    }

    private static void testSetInvalidPx(Path root) throws Exception {
        Path dir = root.resolve("set-bad-px");
        try (StorageEngine engine = new StorageEngine(dir)) {
            CommandHandler handler = new CommandHandler(engine);
            RespValue response = handler.execute(
                    new Command("SET", List.of("key", "value", "PX", "abc")));
            check(response.toString().startsWith("-"),
                    "Invalid PX value should return error");

            RespValue negPx = handler.execute(
                    new Command("SET", List.of("key", "value", "PX", "-5")));
            check(negPx.toString().startsWith("-"),
                    "Negative PX should return error");
        }
        System.out.println("TEST 16 PASSED: Invalid PX argument returns error");
    }

    private static void testMalformedSetCommand(Path root) throws Exception {
        Path dir = root.resolve("malformed-set");
        try (StorageEngine engine = new StorageEngine(dir)) {
            CommandHandler handler = new CommandHandler(engine);

            // SET with only key, no value.
            RespValue noValue = handler.execute(
                    new Command("SET", List.of("key")));
            check(noValue.toString().startsWith("-"),
                    "SET with only key should return error");

            // GET with no args.
            RespValue noArgs = handler.execute(
                    new Command("GET", List.of()));
            check(noArgs.toString().startsWith("-"),
                    "GET with no args should return error");

            // DEL with no args.
            RespValue noDelArgs = handler.execute(
                    new Command("DEL", List.of()));
            check(noDelArgs.toString().startsWith("-"),
                    "DEL with no args should return error");
        }
        System.out.println(
                "TEST 17 PASSED: Malformed commands return errors");
    }

    private static void assertResp(
            String expected, RespValue actual, String testName) {

        String actualStr = actual.toString();
        if (!expected.equals(actualStr)) {
            throw new AssertionError(
                    testName + ": expected " + expected
                            + " but got " + actualStr);
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static void deleteDirectory(Path dir) throws Exception {
        if (Files.exists(dir)) {
            try (var paths = Files.walk(dir)) {
                paths.sorted(Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            } catch (Exception e) {
                                // Best effort cleanup.
                            }
                        });
            }
        }
    }
}
