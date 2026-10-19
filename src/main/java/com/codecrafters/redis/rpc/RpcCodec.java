
package com.codecrafters.redis.rpc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public final class RpcCodec {

    private static final int MAX_FRAME_SIZE = 16 * 1024 * 1024;
    private static final int MAX_STRING_SIZE = 64 * 1024;

    private RpcCodec() {}

    public static void write(
            DataOutputStream out, RpcMessage message)
            throws IOException {

        byte[] body = encodeBody(message);

        if (body.length > MAX_FRAME_SIZE) {
            throw new IOException("RPC frame is too large");
        }

        out.writeInt(body.length);
        out.write(body);
        out.flush();
    }

    public static RpcMessage read(DataInputStream in)
            throws IOException {

        final int frameLength;

        try {
            frameLength = in.readInt();
        } catch (EOFException e) {
            throw e;
        }

        if (frameLength <= 0 || frameLength > MAX_FRAME_SIZE) {
            throw new IOException("Invalid RPC frame length");
        }

        byte[] frame = new byte[frameLength];
        in.readFully(frame);

        try (DataInputStream body =
                     new DataInputStream(
                             new java.io.ByteArrayInputStream(frame))) {

            int typeOrdinal = body.readUnsignedByte();

            RpcMessage.Type[] types = RpcMessage.Type.values();
            if (typeOrdinal >= types.length) {
                throw new IOException("Unknown RPC message type");
            }

            String requestId = readString(body);
            String source = readString(body);
            String target = readString(body);

            int payloadLength = body.readInt();

            if (payloadLength < 0 ||
                    payloadLength > body.available()) {
                throw new IOException("Invalid RPC payload length");
            }

            byte[] payload = new byte[payloadLength];
            body.readFully(payload);

            if (body.available() != 0) {
                throw new IOException("Unexpected trailing RPC data");
            }

            return new RpcMessage(
                    types[typeOrdinal],
                    requestId,
                    source,
                    target,
                    payload);
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid RPC message", e);
        }
    }

    private static byte[] encodeBody(RpcMessage message)
            throws IOException {

        java.io.ByteArrayOutputStream bytes =
                new java.io.ByteArrayOutputStream();

        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeByte(message.getType().ordinal());
            writeString(out, message.getRequestId());
            writeString(out, message.getSourceNodeId());
            writeString(out, message.getTargetNodeId());

            byte[] payload = message.getPayload();
            out.writeInt(payload.length);
            out.write(payload);
        }

        return bytes.toByteArray();
    }

    private static void writeString(
            DataOutputStream out, String value)
            throws IOException {

        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);

        if (bytes.length > MAX_STRING_SIZE) {
            throw new IOException("RPC string is too long");
        }

        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static String readString(DataInputStream in)
            throws IOException {

        int length = in.readInt();

        if (length < 0 || length > MAX_STRING_SIZE
                || length > in.available()) {
            throw new IOException("Invalid RPC string length");
        }

        byte[] bytes = new byte[length];
        in.readFully(bytes);

        return new String(bytes, StandardCharsets.UTF_8);
    }
}
