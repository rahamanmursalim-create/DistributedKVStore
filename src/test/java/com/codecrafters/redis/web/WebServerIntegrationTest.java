package com.codecrafters.redis.web;

import com.codecrafters.redis.storage.RedisStore;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class WebServerIntegrationTest {

    private static final int PORT = 18080;

    public static void main(String[] args) throws Exception {
        try (RedisStore store = new RedisStore();
             WebServer webServer = new WebServer(store, PORT)) {

            webServer.start();
            Thread.sleep(150);

            HttpClient client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(2))
                    .build();

            testDashboardHtml(client);
            testHealthEndpoint(client);
            testPutAndGetKey(client);
            testListKeys(client);
            testDeleteKey(client);
        }

        System.out.println("WebServerIntegrationTest passed all tests.");
    }

    private static void testDashboardHtml(HttpClient client) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + PORT + "/"))
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

    private static void testHealthEndpoint(HttpClient client) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + PORT + "/api/health"))
                .GET()
                .build();

        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        assert resp.statusCode() == 200 : "Health should return 200";
        assert resp.body().contains("\"status\":\"ok\"") : "Health body should contain ok status";
    }

    private static void testPutAndGetKey(HttpClient client) throws Exception {
        // PUT key
        HttpRequest putReq = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + PORT + "/api/keys/cluster%3Aconfig"))
                .PUT(HttpRequest.BodyPublishers.ofString("{\"shards\":3,\"mode\":\"neumorphic\"}"))
                .header("Content-Type", "text/plain;charset=UTF-8")
                .build();

        HttpResponse<String> putResp = client.send(putReq, HttpResponse.BodyHandlers.ofString());
        assert putResp.statusCode() == 200 : "PUT should return 200, got " + putResp.statusCode();

        // GET key
        HttpRequest getReq = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + PORT + "/api/keys/cluster%3Aconfig"))
                .GET()
                .build();

        HttpResponse<String> getResp = client.send(getReq, HttpResponse.BodyHandlers.ofString());
        assert getResp.statusCode() == 200 : "GET should return 200, got " + getResp.statusCode();
        assert getResp.body().contains("cluster:config") : "GET body should contain key";
        assert getResp.body().contains("neumorphic") : "GET body should contain value";
    }

    private static void testListKeys(HttpClient client) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + PORT + "/api/keys"))
                .GET()
                .build();

        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        assert resp.statusCode() == 200 : "Keys list should return 200";
        assert resp.body().contains("cluster:config") : "Keys list should contain created key";
    }

    private static void testDeleteKey(HttpClient client) throws Exception {
        HttpRequest delReq = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + PORT + "/api/keys/cluster%3Aconfig"))
                .DELETE()
                .build();

        HttpResponse<String> delResp = client.send(delReq, HttpResponse.BodyHandlers.ofString());
        assert delResp.statusCode() == 200 : "DELETE should return 200";

        // Verify key is now 404
        HttpRequest getReq = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + PORT + "/api/keys/cluster%3Aconfig"))
                .GET()
                .build();

        HttpResponse<String> getResp = client.send(getReq, HttpResponse.BodyHandlers.ofString());
        assert getResp.statusCode() == 404 : "Deleted key should return 404, got " + getResp.statusCode();
    }
}

