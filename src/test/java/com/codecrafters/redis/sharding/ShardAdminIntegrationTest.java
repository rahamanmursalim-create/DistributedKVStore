package com.codecrafters.redis.sharding;

import com.codecrafters.redis.web.WebServer;
import com.codecrafters.redis.storage.RedisStore;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;

public final class ShardAdminIntegrationTest {
    public static void main(String[] args) throws Exception {
        Path root = Files.createTempDirectory("shard-admin-test-");
        RedisStore primary = new RedisStore(root.resolve("wal.log"), false);
        ShardedStore sharded = new ShardedStore(32);
        sharded.addNode("one", primary);
        for (int i = 0; i < 300; i++) sharded.set("key-" + i, "value-" + i);

        ShardAdminService admin = new ShardAdminService(sharded, root, "test-secret", "wal");
        try (WebServer server = new WebServer(primary, sharded, 0, null, false, admin)) {
            server.start();
            HttpClient client = HttpClient.newHttpClient();
            URI base = URI.create("http://127.0.0.1:" + server.getPort() + "/api/shards");

            HttpResponse<String> unauthorized = client.send(
                    HttpRequest.newBuilder(base).GET().build(), HttpResponse.BodyHandlers.ofString());
            assert unauthorized.statusCode() == 401;

            HttpResponse<String> listed = send(client, base, "GET", "", "test-secret");
            assert listed.statusCode() == 200 && listed.body().contains("\"one\"");
            HttpResponse<String> added = send(client, base, "POST", "/two", "test-secret");
            assert added.statusCode() == 201;
            assert send(client, base, "POST", "/two", "test-secret").statusCode() == 400;
            assert sharded.nodeIds().contains("two");
            var persistedMembership = Files.readAllLines(root.resolve("shard-membership.tsv"));
            assert persistedMembership.contains("one\tlegacy");
            assert persistedMembership.contains("two\tshards/two");
            for (int i = 0; i < 300; i++) assert ("value-" + i).equals(sharded.get("key-" + i));

            HttpResponse<String> removed = send(client, base, "DELETE", "/two", "test-secret");
            assert removed.statusCode() == 200;
            assert !sharded.nodeIds().contains("two");
            assert Files.readAllLines(root.resolve("shard-membership.tsv")).equals(java.util.List.of("one\tlegacy"));
            for (int i = 0; i < 300; i++) assert ("value-" + i).equals(sharded.get("key-" + i));

            assert send(client, base, "POST", "/two", "test-secret").statusCode() == 201;
            assert send(client, base, "DELETE", "/one", "test-secret").statusCode() == 200;
            assert Files.readAllLines(root.resolve("shard-membership.tsv")).equals(java.util.List.of("two\tshards/two"));
        } finally {
            sharded.close();
        }
        try (RedisStore recovered = new RedisStore(root.resolve("shards/two/wal.log"), false)) {
            for (int i = 0; i < 300; i++) assert ("value-" + i).equals(recovered.get("key-" + i));
        }
        System.out.println("ShardAdminIntegrationTest passed");
    }

    private static HttpResponse<String> send(HttpClient client, URI base, String method,
                                             String suffix, String token) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(base + suffix))
                .header("Authorization", "Bearer " + token)
                .method(method, HttpRequest.BodyPublishers.noBody())
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
