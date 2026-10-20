package com.distributedkv.network.protocol.codec;

import com.distributedkv.common.Result;
import com.distributedkv.network.protocol.Message;

/**
 * MessageCodec defines the interface for encoding and decoding messages.
 * Different implementations can use different serialization formats like Protobuf,
 * JSON, or custom binary formats.
 */
public interface MessageCodec {

    /**
     * Encodes a message into a byte array.
     *
     * @param message The message to encode
     * @return The encoded byte array
     * @throws Exception If an error occurs during encoding
     */
    byte[] encode(Message message) throws Exception;

    /**
     * Decodes a byte array into a message.
     *
     * @param bytes The byte array to decode
     * @return The decoded message
     * @throws Exception If an error occurs during decoding
     */
    Message decode(byte[] bytes) throws Exception;

    /**
     * Encodes a message into a byte array with error handling.
     *
     * @param message The message to encode
     * @return A result containing the encoded byte array or an error
     */
    default Result<byte[]> encodeWithResult(Message message) {
        try {
            byte[] bytes = encode(message);
            return Result.success(bytes);
        } catch (Exception e) {
            return Result.failure(com.distributedkv.common.Status.INTERNAL_ERROR,
                               "Failed to encode message: " + e.getMessage());
        }
    }

    /**
     * Decodes a byte array into a message with error handling.
     *
     * @param bytes The byte array to decode
     * @return A result containing the decoded message or an error
     */
    default Result<Message> decodeWithResult(byte[] bytes) {
        try {
            Message message = decode(bytes);
            return Result.success(message);
        } catch (Exception e) {
            return Result.failure(com.distributedkv.common.Status.INTERNAL_ERROR,
                               "Failed to decode message: " + e.getMessage());
        }
    }

    /**
     * Gets the content type of the encoded data.
     *
     * @return The content type string (e.g., "application/protobuf", "application/json")
     */
    String getContentType();
}