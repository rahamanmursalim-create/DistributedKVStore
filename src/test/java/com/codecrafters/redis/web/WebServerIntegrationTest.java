package com.codecrafters.redis.web;

import org.junit.jupiter.api.Test;

import com.codecrafters.redis.storage.RedisStore;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.ConnectException;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import com.codecrafters.redis.rpc.TlsContext;
import javax.net.ssl.SSLContext;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.net.ssl.SSLHandshakeException;

public class WebServerIntegrationTest {

    private static final String LOOPBACK = "127.0.0.1";

    public static void main(String[] args) throws Exception {
        int dashboardPort;
        try (RedisStore store = new RedisStore();
             WebServer webServer = new WebServer(store, 0)) {

            webServer.start();
            dashboardPort = webServer.getPort();
            awaitListener(dashboardPort);

            HttpClient client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(5))
                    .build();

            testDashboardHtml(client, dashboardPort);
            testHealthEndpoint(client, dashboardPort);
            testPutAndGetKey(client, dashboardPort);
            testListKeys(client, dashboardPort);
            testDeleteKey(client, dashboardPort);
        }

        try (RedisStore protectedStore = new RedisStore();
             WebServer protectedServer = new WebServer(protectedStore, protectedStore, 0, null, false, null, "test-secret")) {
            protectedServer.start();
            awaitListener(protectedServer.getPort());
            HttpClient client = plainClient();
            URI health = URI.create("http://" + LOOPBACK + ":" + protectedServer.getPort() + "/api/health");
            HttpResponse<String> denied = client.send(HttpRequest.newBuilder(health).GET().build(), HttpResponse.BodyHandlers.ofString());
            assert denied.statusCode() == 401 : "Protected HTTP API accepted an unauthenticated request";
            HttpRequest allowedRequest = HttpRequest.newBuilder(health).header("Authorization", "Bearer test-secret").GET().build();
            HttpResponse<String> allowed = client.send(allowedRequest, HttpResponse.BodyHandlers.ofString());
            assert allowed.statusCode() == 200 : "Valid bearer token was rejected";
            URI metrics = URI.create("http://" + LOOPBACK + ":" + protectedServer.getPort() + "/api/metrics");
            HttpResponse<String> metricsDenied = client.send(HttpRequest.newBuilder(metrics).GET().build(), HttpResponse.BodyHandlers.ofString());
            assert metricsDenied.statusCode() == 401 : "Metrics endpoint must require client authentication";
            HttpResponse<String> metricsAllowed = client.send(HttpRequest.newBuilder(metrics)
                    .header("Authorization", "Bearer test-secret").GET().build(), HttpResponse.BodyHandlers.ofString());
            assert metricsAllowed.statusCode() == 200 : "Authenticated metrics scrape failed";
            assert metricsAllowed.body().contains("kv_process_up 1") : "Metrics must expose process availability";
            assert java.util.regex.Pattern.compile("(?m)^kv_store_keys \\d+$").matcher(metricsAllowed.body()).find()
                    : "Metrics must expose a numeric live key count";
        }

        testHttpsAuthenticationAndCertificateRotation();

        System.out.println("WebServerIntegrationTest passed all tests.");
    }

    private static void testHttpsAuthenticationAndCertificateRotation() throws Exception {
        Path dir = Files.createTempDirectory("https-e2e-test");
        char[] password = "changeit-test".toCharArray();
        try {
            Path first = dir.resolve("first.p12"), second = dir.resolve("second.p12");
            keytool(first, "first", password);
            keytool(second, "second", password);
            SSLContext firstContext = TlsContext.fromStores(first, password, first, password, "PKCS12", true);
            SSLContext secondContext = TlsContext.fromStores(second, password, second, password, "PKCS12", true);
            SSLContext firstOnlyTrust = TlsContext.fromStores(null, password, first, password, "PKCS12", false);
            SSLContext secondOnlyTrust = TlsContext.fromStores(null, password, second, password, "PKCS12", false);

            try (RedisStore store = new RedisStore();
                 WebServer server = new WebServer(store, store, 0, null, false, null, "https-secret", firstContext)) {
                server.start();
                awaitListener(server.getPort());
                URI health = URI.create("https://" + LOOPBACK + ":" + server.getPort() + "/api/health");
                HttpClient trusted = client(firstOnlyTrust);
                HttpClient untrusted = client(secondOnlyTrust);
                HttpResponse<String> noToken = trusted.send(HttpRequest.newBuilder(health).GET().build(), HttpResponse.BodyHandlers.ofString());
                assert noToken.statusCode() == 401 : "HTTPS endpoint accepted missing bearer token";
                HttpResponse<String> valid = trusted.send(HttpRequest.newBuilder(health)
                        .header("Authorization", "Bearer https-secret").GET().build(), HttpResponse.BodyHandlers.ofString());
                assert valid.statusCode() == 200 : "HTTPS + valid bearer token failed";
                expectCertificateRejection(untrusted, health, "untrusted certificate");
                expectPlaintextRejection(health, server.getPort());
            }

            // A restarted listener must use the rotated leaf certificate; old trust fails closed.
            try (RedisStore store = new RedisStore();
                 WebServer rotated = new WebServer(store, store, 0, null, false, null, "https-secret", secondContext)) {
                rotated.start();
                awaitListener(rotated.getPort());
                URI health = URI.create("https://" + LOOPBACK + ":" + rotated.getPort() + "/api/health");
                expectCertificateRejection(client(firstOnlyTrust), health, "previous certificate after rotation");
                assert client(secondOnlyTrust).send(HttpRequest.newBuilder(health).header("Authorization", "Bearer https-secret").GET().build(), HttpResponse.BodyHandlers.ofString()).statusCode() == 200;
            }
        } finally {
            try (var paths = Files.walk(dir)) {
                paths.sorted(java.util.Comparator.reverseOrder()).forEach(path -> { try { Files.deleteIfExists(path); } catch (Exception e) { throw new RuntimeException(e); } });
            }
        }
    }

    private static HttpClient client(SSLContext context) {
        return HttpClient.newBuilder().sslContext(context).connectTimeout(Duration.ofSeconds(5)).build();
    }

    private static HttpClient plainClient() {
        return HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    private static void awaitListener(int port) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        IOException lastFailure = null;
        while (System.nanoTime() < deadline) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(LOOPBACK, port), 500);
                return;
            } catch (ConnectException notReady) {
                lastFailure = notReady;
                Thread.sleep(25);
            }
        }
        throw new AssertionError("Web listener did not accept loopback connections on port " + port, lastFailure);
    }

    private static void expectCertificateRejection(HttpClient client, URI uri, String scenario) throws Exception {
        try {
            client.send(HttpRequest.newBuilder(uri).header("Authorization", "Bearer https-secret").GET().build(),
                    HttpResponse.BodyHandlers.ofString());
        } catch (IOException failure) {
            if (hasCause(failure, SSLHandshakeException.class)) return;
            throw new AssertionError("TLS rejection for " + scenario + " failed for an unrelated I/O reason", failure);
        }
        throw new AssertionError("HTTPS endpoint accepted " + scenario);
    }

    private static void expectPlaintextRejection(URI httpsUri, int port) throws Exception {
        awaitListener(port);
        URI plaintextUri = URI.create("http://" + LOOPBACK + ":" + port + "/api/health");
        try {
            HttpResponse<String> response = plainClient().send(HttpRequest.newBuilder(plaintextUri).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            throw new AssertionError("TLS-only endpoint returned HTTP status " + response.statusCode()
                    + " to plaintext traffic (HTTPS URI was " + httpsUri + ")");
        } catch (IOException failure) {
            if (hasCause(failure, HttpConnectTimeoutException.class)
                    || hasCause(failure, ConnectException.class)) {
                throw new AssertionError("Plaintext rejection check could not connect to the listening TLS port", failure);
            }
            // A connected TLS listener must reject an HTTP request; protocol
            // parsing/connection-close errors are expected, connect failures are not.
        }
    }

    private static boolean hasCause(Throwable failure, Class<? extends Throwable> type) {
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (type.isInstance(current)) return true;
        }
        return false;
    }

    private static void keytool(Path file, String alias, char[] password) throws Exception {
        String executable = Path.of(System.getProperty("java.home"), "bin",
                System.getProperty("os.name").toLowerCase().contains("win") ? "keytool.exe" : "keytool").toString();
        Process process = new ProcessBuilder(executable, "-genkeypair", "-alias", alias,
                "-keyalg", "RSA", "-keysize", "2048", "-validity", "2", "-dname", "CN=localhost",
                "-ext", "SAN=dns:localhost,ip:127.0.0.1", "-storetype", "PKCS12", "-keystore", file.toString(),
                "-storepass", new String(password), "-keypass", new String(password), "-noprompt").redirectErrorStream(true).start();
        byte[] output = process.getInputStream().readAllBytes();
        if (process.waitFor() != 0) throw new IllegalStateException("keytool failed: " + new String(output));
    }

    private static void testDashboardHtml(HttpClient client, int port) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://" + LOOPBACK + ":" + port + "/"))
                .GET()
                .build();

        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        assert resp.statusCode() == 200 : "Expected 200, got " + resp.statusCode();
        String body = resp.body();
        assert body.contains("DistributedKVStore") : "Dashboard should contain title";
        assert body.contains("--surface-neu") : "Dashboard should contain dark neumorphic CSS";
        assert body.contains("deleteModal") : "Dashboard should contain fluid delete modal";
        assert body.contains("keysSkeleton") : "Dashboard should contain loading skeleton";
        assert body.contains("copyValue()") : "Dashboard should contain Copy Value button";
        assert body.contains("copyToClipboard") : "Dashboard should contain copyToClipboard helper";
        assert body.contains("formatJson()") : "Dashboard should contain formatJson helper";
    }

    private static void testHealthEndpoint(HttpClient client, int port) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://" + LOOPBACK + ":" + port + "/api/health"))
                .GET()
                .build();

        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        assert resp.statusCode() == 200 : "Health should return 200";
        assert resp.body().contains("\"status\":\"ok\"") : "Health body should contain ok status";
    }

    private static void testPutAndGetKey(HttpClient client, int port) throws Exception {
        // PUT key
        HttpRequest putReq = HttpRequest.newBuilder()
                .uri(URI.create("http://" + LOOPBACK + ":" + port + "/api/keys/cluster%3Aconfig"))
                .PUT(HttpRequest.BodyPublishers.ofString("{\"shards\":3,\"mode\":\"neumorphic\"}"))
                .header("Content-Type", "text/plain;charset=UTF-8")
                .build();

        HttpResponse<String> putResp = client.send(putReq, HttpResponse.BodyHandlers.ofString());
        assert putResp.statusCode() == 200 : "PUT should return 200, got " + putResp.statusCode();

        // GET key
        HttpRequest getReq = HttpRequest.newBuilder()
                .uri(URI.create("http://" + LOOPBACK + ":" + port + "/api/keys/cluster%3Aconfig"))
                .GET()
                .build();

        HttpResponse<String> getResp = client.send(getReq, HttpResponse.BodyHandlers.ofString());
        assert getResp.statusCode() == 200 : "GET should return 200, got " + getResp.statusCode();
        assert getResp.body().contains("cluster:config") : "GET body should contain key";
        assert getResp.body().contains("neumorphic") : "GET body should contain value";
    }

    private static void testListKeys(HttpClient client, int port) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://" + LOOPBACK + ":" + port + "/api/keys"))
                .GET()
                .build();

        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        assert resp.statusCode() == 200 : "Keys list should return 200";
        assert resp.body().contains("cluster:config") : "Keys list should contain created key";
    }

    private static void testDeleteKey(HttpClient client, int port) throws Exception {
        HttpRequest delReq = HttpRequest.newBuilder()
                .uri(URI.create("http://" + LOOPBACK + ":" + port + "/api/keys/cluster%3Aconfig"))
                .DELETE()
                .build();

        HttpResponse<String> delResp = client.send(delReq, HttpResponse.BodyHandlers.ofString());
        assert delResp.statusCode() == 200 : "DELETE should return 200";

        // Verify key is now 404
        HttpRequest getReq = HttpRequest.newBuilder()
                .uri(URI.create("http://" + LOOPBACK + ":" + port + "/api/keys/cluster%3Aconfig"))
                .GET()
                .build();

        HttpResponse<String> getResp = client.send(getReq, HttpResponse.BodyHandlers.ofString());
        assert getResp.statusCode() == 404 : "Deleted key should return 404, got " + getResp.statusCode();
    }

    @Test
    void runsExistingScenarioUnderJUnit() throws Exception {
        main(new String[0]);
    }
}

