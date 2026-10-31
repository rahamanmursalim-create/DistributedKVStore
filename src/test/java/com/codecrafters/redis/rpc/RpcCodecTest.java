package com.codecrafters.redis.rpc;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class RpcCodecTest {

    public static void main(String[] args) throws Exception {
        testEncodeDecodeRoundTrip();
        testInvalidPayloadLength();
        System.out.println("ALL RPC CODEC TESTS PASSED");
    }

    private static void testEncodeDecodeRoundTrip() throws Exception {
        byte[] payload = "test payload data".getBytes(StandardCharsets.UTF_8);
        RpcMessage msg = new RpcMessage(
                RpcMessage.Type.APPEND_ENTRIES_REQUEST,
                "req-123",
                "node-1",
                "node-2",
                payload
        );

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(baos)) {
            RpcCodec.write(out, msg);
        }

        byte[] serialized = baos.toByteArray();
        assert serialized.length > 0 : "Serialized frame should not be empty";

        ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
        RpcMessage decoded;
        try (DataInputStream in = new DataInputStream(bais)) {
            decoded = RpcCodec.read(in);
        }

        assert decoded.getType() == RpcMessage.Type.APPEND_ENTRIES_REQUEST : "Type mismatch";
        assert "req-123".equals(decoded.getRequestId()) : "RequestId mismatch";
        assert "node-1".equals(decoded.getSourceNodeId()) : "SourceNodeId mismatch";
        assert "node-2".equals(decoded.getTargetNodeId()) : "TargetNodeId mismatch";
        assert Arrays.equals(payload, decoded.getPayload()) : "Payload mismatch";
        System.out.println("TEST 1 PASSED: RPC round-trip serialization");
    }

    private static void testInvalidPayloadLength() {
        try {
            new RpcMessage(RpcMessage.Type.HEARTBEAT, null, "src", "tgt", new byte[0]);
            throw new AssertionError("Should reject null requestId");
        } catch (IllegalArgumentException expected) {
        }
        System.out.println("TEST 2 PASSED: RPC message validation");
    }

    @Test
    void runsExistingScenarioUnderJUnit() throws Exception {
        main(new String[0]);
    }
}
