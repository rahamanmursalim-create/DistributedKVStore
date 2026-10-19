package com.codecrafters.redis;

import com.codecrafters.redis.protocol.RespReader;
import com.codecrafters.redis.protocol.RespType;
import com.codecrafters.redis.protocol.RespValue;
import com.codecrafters.redis.protocol.RespWriter;
import com.codecrafters.redis.storage.RedisStore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RedisServerTest {

    private static int testsPassed = 0;
    private static int testsFailed = 0;

    public static void main(String[] args) throws Exception {
        System.out.println("=================================================");
        System.out.println("Running Redis Server & RESP Protocol Test Suite");
        System.out.println("=================================================\n");

        testRespSerialization();
        testRespParsing();
        testRespFragmentedParsing();
        testRespRejectsOversizedLengths();
        testRedisStoreBasic();
        testRedisStoreExpiry();
        testLiveServerStages();

        System.out.println("\n=================================================");
        System.out.printf("Test Results: %d Passed, %d Failed%n", testsPassed, testsFailed);
        System.out.println("=================================================");

        if (testsFailed > 0) {
            System.exit(1);
        }
    }

    private static void assertTrue(String testName, boolean condition, String message) {
        if (condition) {
            System.out.printf(" [PASS] %s%n", testName);
            testsPassed++;
        } else {
            System.err.printf("![FAIL] %s: %s%n", testName, message);
            testsFailed++;
        }
    }

    private static void assertEquals(String testName, Object expected, Object actual) {
        if ((expected == null && actual == null) || (expected != null && expected.equals(actual))) {
            System.out.printf(" [PASS] %s%n", testName);
            testsPassed++;
        } else {
            System.err.printf("![FAIL] %s: expected '%s', got '%s'%n", testName, expected, actual);
            testsFailed++;
        }
    }

    private static void testRespSerialization() throws IOException {
        System.out.println("--- Test RESP Serialization ---");

        // Simple String
        RespValue ping = RespValue.simpleString("PONG");
        assertEquals("Encode PONG simple string", "+PONG\r\n", new String(ping.encode(), StandardCharsets.UTF_8));

        // Error
        RespValue err = RespValue.error("ERR unknown");
        assertEquals("Encode error", "-ERR unknown\r\n", new String(err.encode(), StandardCharsets.UTF_8));

        // Integer
        RespValue num = RespValue.integer(42);
        assertEquals("Encode integer", ":42\r\n", new String(num.encode(), StandardCharsets.UTF_8));

        // Bulk String
        RespValue bulk = RespValue.bulkString("hello");
        assertEquals("Encode bulk string", "$5\r\nhello\r\n", new String(bulk.encode(), StandardCharsets.UTF_8));

        // Null Bulk String
        RespValue nullBulk = RespValue.nullBulkString();
        assertEquals("Encode null bulk string", "$-1\r\n", new String(nullBulk.encode(), StandardCharsets.UTF_8));

        // Array
        List<RespValue> list = new ArrayList<>();
        list.add(RespValue.bulkString("ECHO"));
        list.add(RespValue.bulkString("world"));
        RespValue arr = RespValue.array(list);
        assertEquals("Encode array", "*2\r\n$4\r\nECHO\r\n$5\r\nworld\r\n", new String(arr.encode(), StandardCharsets.UTF_8));
    }

    private static void testRespParsing() throws IOException {
        System.out.println("\n--- Test RESP Parsing ---");

        // Parse array of bulk strings
        String raw = "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n";
        RespReader reader = new RespReader(new ByteArrayInputStream(raw.getBytes(StandardCharsets.UTF_8)));
        RespValue val = reader.read();

        assertTrue("Parsed array is not null", val != null, "Parsed value was null");
        assertEquals("Parsed array type", RespType.ARRAY, val.getType());
        assertEquals("Parsed array size", 2, val.getArrayValue().size());
        assertEquals("Parsed array elem 0", "ECHO", val.getArrayValue().get(0).getStringValue());
        assertEquals("Parsed array elem 1", "hey", val.getArrayValue().get(1).getStringValue());

        // Parse inline command
        String inline = "PING mymsg\r\n";
        RespReader inlineReader = new RespReader(new ByteArrayInputStream(inline.getBytes(StandardCharsets.UTF_8)));
        RespValue inlineVal = inlineReader.read();
        assertTrue("Parsed inline is not null", inlineVal != null, "Inline value was null");
        assertEquals("Parsed inline size", 2, inlineVal.getArrayValue().size());
        assertEquals("Parsed inline elem 0", "PING", inlineVal.getArrayValue().get(0).getStringValue());
        assertEquals("Parsed inline elem 1", "mymsg", inlineVal.getArrayValue().get(1).getStringValue());
    }

    private static void testRespFragmentedParsing() throws IOException {
        System.out.println("\n--- Test RESP Fragmented TCP Stream ---");

        // Simulate bytes arriving byte-by-byte or in small chunks
        byte[] bytes = "*2\r\n$4\r\nECHO\r\n$11\r\nhello world\r\n".getBytes(StandardCharsets.UTF_8);

        InputStream slowIn = new InputStream() {
            private int pos = 0;
            @Override
            public int read() {
                if (pos < bytes.length) {
                    return bytes[pos++] & 0xFF;
                }
                return -1;
            }
        };

        RespReader reader = new RespReader(slowIn);
        RespValue val = reader.read();
        assertTrue("Parsed fragmented stream not null", val != null, "Value was null");
        assertEquals("Parsed fragmented command", "ECHO", val.getArrayValue().get(0).getStringValue());
        assertEquals("Parsed fragmented arg", "hello world", val.getArrayValue().get(1).getStringValue());
    }

    private static void testRespRejectsOversizedLengths() throws IOException {
        System.out.println("\n--- Test RESP Input Limits ---");

        assertRespRejected("oversized bulk string", "$67108865\\r\\n");
        assertRespRejected("oversized array", "*1000001\\r\\n");
        assertRespRejected("malformed length", "$not-a-number\\r\\n");
        assertRespRejected("oversized line", "+" + "x".repeat(65_537) + "\\r\\n");
    }

    private static void assertRespRejected(String name, String input) throws IOException {
        try {
            new RespReader(new ByteArrayInputStream(
                    input.getBytes(StandardCharsets.UTF_8))).read();
            assertTrue("Reject " + name, false, "Input was accepted");
        } catch (IOException expected) {
            assertTrue("Reject " + name, true, "");
        }
    }

    private static void testRedisStoreBasic() throws IOException {
        System.out.println("\n--- Test RedisStore In-Memory & Commands ---");
        try (RedisStore store = new RedisStore(
                Files.createTempDirectory("redis-store-basic-").resolve("wal.log"), false)) {
            store.set("foo", "bar");
            assertEquals("Get existing key", "bar", store.get("foo"));
            assertEquals("Get non-existing key", null, store.get("nonexistent"));

            store.del("foo");
            assertEquals("Get deleted key", null, store.get("foo"));
        }
    }

    private static void testRedisStoreExpiry() throws IOException, InterruptedException {
        System.out.println("\n--- Test RedisStore Key Expiry (PX) ---");
        try (RedisStore store = new RedisStore(
                Files.createTempDirectory("redis-store-expiry-").resolve("wal.log"), false)) {
            // Set with 100ms expiry
            store.set("tempKey", "tempValue", 100);
            assertEquals("Get before expiry", "tempValue", store.get("tempKey"));

            // Wait for expiration
            Thread.sleep(150);

            assertEquals("Get after expiry (passive eviction)", null, store.get("tempKey"));
            assertTrue("Key is removed from store", !store.exists("tempKey"), "Key still exists after expiry");
        }
    }

    private static void testLiveServerStages() throws Exception {
        System.out.println("\n--- Test Live Server End-to-End Across All Stages ---");

        int testPort = 6380;
        RedisStore serverStore = new RedisStore(
                Files.createTempDirectory("redis-server-integration-").resolve("wal.log"), false);
        Server server = new Server(testPort, serverStore);

        Thread serverThread = new Thread(() -> {
            try {
                server.start();
            } catch (IOException e) {
                // Stopped
            }
        });
        serverThread.start();

        // Give server a moment to start
        Thread.sleep(100);

        try {
            // Stage 1 & 2: PING
            try (Socket socket = new Socket("localhost", testPort)) {
                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();
                RespReader reader = new RespReader(in);

                // Send PING as RESP Array
                out.write("*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.UTF_8));
                out.flush();

                RespValue resp = reader.read();
                assertTrue("PING response not null", resp != null, "Response was null");
                assertEquals("Stage 1/2: PING response", "PONG", resp.getStringValue());

                // Send PING with message
                out.write("*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n".getBytes(StandardCharsets.UTF_8));
                out.flush();
                RespValue pingMsgResp = reader.read();
                assertEquals("PING with argument response", "hello", pingMsgResp.getStringValue());
            }

            // Stage 3 & 4: ECHO Command & Pipelined Requests
            try (Socket socket = new Socket("localhost", testPort)) {
                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();
                RespReader reader = new RespReader(in);

                out.write("*2\r\n$4\r\nECHO\r\n$11\r\nhello world\r\n".getBytes(StandardCharsets.UTF_8));
                out.flush();

                RespValue resp = reader.read();
                assertTrue("ECHO response not null", resp != null, "Response was null");
                assertEquals("Stage 4: ECHO response", "hello world", resp.getStringValue());
            }

            // Stage 5: SET & GET Commands
            try (Socket socket = new Socket("localhost", testPort)) {
                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();
                RespReader reader = new RespReader(in);

                // SET key value
                out.write("*3\r\n$3\r\nSET\r\n$9\r\nmyTestKey\r\n$11\r\nmyTestValue\r\n".getBytes(StandardCharsets.UTF_8));
                out.flush();
                RespValue setResp = reader.read();
                assertEquals("Stage 5: SET response", "OK", setResp.getStringValue());

                // GET key
                out.write("*2\r\n$3\r\nGET\r\n$9\r\nmyTestKey\r\n".getBytes(StandardCharsets.UTF_8));
                out.flush();
                RespValue getResp = reader.read();
                assertEquals("Stage 5: GET response", "myTestValue", getResp.getStringValue());

                // GET nonexistent key -> Null Bulk String ($-1\r\n)
                out.write("*2\r\n$3\r\nGET\r\n$11\r\nmissing_key\r\n".getBytes(StandardCharsets.UTF_8));
                out.flush();
                RespValue getMissingResp = reader.read();
                assertTrue("Stage 5: GET missing key is Null Bulk String", getMissingResp.isNull(), "Expected null bulk string");
            }

            // Stage 6: Key Expiry (PX)
            try (Socket socket = new Socket("localhost", testPort)) {
                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();
                RespReader reader = new RespReader(in);

                // SET key with PX 150ms
                out.write("*5\r\n$3\r\nSET\r\n$9\r\nexpiringK\r\n$9\r\nexpiringV\r\n$2\r\npx\r\n$3\r\n150\r\n".getBytes(StandardCharsets.UTF_8));
                out.flush();
                RespValue setPxResp = reader.read();
                assertEquals("Stage 6: SET with PX response", "OK", setPxResp.getStringValue());

                // Immediate GET (should exist)
                out.write("*2\r\n$3\r\nGET\r\n$9\r\nexpiringK\r\n".getBytes(StandardCharsets.UTF_8));
                out.flush();
                RespValue getImmediate = reader.read();
                assertEquals("Stage 6: GET before PX expiry", "expiringV", getImmediate.getStringValue());

                // Wait for expiration
                Thread.sleep(200);

                // GET after expiration (should return $-1\r\n)
                out.write("*2\r\n$3\r\nGET\r\n$9\r\nexpiringK\r\n".getBytes(StandardCharsets.UTF_8));
                out.flush();
                RespValue getExpired = reader.read();
                assertTrue("Stage 6: GET after PX expiry returns Null Bulk String", getExpired.isNull(), "Expected null bulk string after expiry");
            }

            // Stage 3: Concurrent Clients
            System.out.println("--- Testing 50 Concurrent Clients in Parallel ---");
            int clientCount = 50;
            ExecutorService clientPool = Executors.newFixedThreadPool(10);
            CountDownLatch latch = new CountDownLatch(clientCount);
            AtomicInteger concurrentSuccesses = new AtomicInteger(0);

            for (int i = 0; i < clientCount; i++) {
                final int clientId = i;
                clientPool.submit(() -> {
                    try (Socket socket = new Socket("localhost", testPort)) {
                        OutputStream out = socket.getOutputStream();
                        InputStream in = socket.getInputStream();
                        RespReader reader = new RespReader(in);

                        // SET unique key
                        String key = "concurrent_key_" + clientId;
                        String val = "concurrent_val_" + clientId;
                        out.write(("*3\r\n$3\r\nSET\r\n$" + key.length() + "\r\n" + key + "\r\n$" + val.length() + "\r\n" + val + "\r\n").getBytes(StandardCharsets.UTF_8));
                        out.flush();
                        RespValue setRes = reader.read();

                        // GET unique key
                        out.write(("*2\r\n$3\r\nGET\r\n$" + key.length() + "\r\n" + key + "\r\n").getBytes(StandardCharsets.UTF_8));
                        out.flush();
                        RespValue getRes = reader.read();

                        if ("OK".equals(setRes.getStringValue()) && val.equals(getRes.getStringValue())) {
                            concurrentSuccesses.incrementAndGet();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                });
            }

            boolean completedInTime = latch.await(10, TimeUnit.SECONDS);
            clientPool.shutdown();
            assertTrue("Concurrent clients completed in time", completedInTime, "Timeout waiting for concurrent clients");
            assertEquals("Stage 3: All concurrent client transactions succeeded", clientCount, concurrentSuccesses.get());

        } finally {
            server.stop();
            serverThread.join(2000);
            serverStore.close();
        }
    }
}
