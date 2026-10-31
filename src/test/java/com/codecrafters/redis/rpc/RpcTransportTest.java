package com.codecrafters.redis.rpc;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.UUID;

public final class RpcTransportTest {
    public static void main(String[] args) throws Exception {
        try (RpcTransportServer server = new RpcTransportServer(
                new InetSocketAddress("127.0.0.1", 0), 2, 2_000, request ->
                new RpcMessage(RpcMessage.Type.RESPONSE, request.getRequestId(),
                        "server", request.getSourceNodeId(), request.getPayload()))) {
            server.start();
            RpcTransportClient client = new RpcTransportClient(
                    new InetSocketAddress("127.0.0.1", server.getPort()), 1_000, 2_000);
            byte[] payload = new byte[128 * 1024];
            for (int i = 0; i < payload.length; i++) payload[i] = (byte) (i * 31);
            RpcMessage request = new RpcMessage(RpcMessage.Type.APPEND_ENTRIES_REQUEST,
                    UUID.randomUUID().toString(), "client", "server", payload);
            RpcMessage response = client.call(request);
            assert response.getType() == RpcMessage.Type.RESPONSE;
            assert response.getSourceNodeId().equals("server");
            assert response.getTargetNodeId().equals("client");
            assert response.getRequestId().equals(request.getRequestId());
            assert Arrays.equals(response.getPayload(), payload);
        }
        System.out.println("RpcTransportTest passed");
    }

    @Test
    void runsExistingScenarioUnderJUnit() throws Exception {
        main(new String[0]);
    }
}
