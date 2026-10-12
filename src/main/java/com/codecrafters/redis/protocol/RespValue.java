package com.codecrafters.redis.protocol;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Representation of a RESP (REdis Serialization Protocol) value.
 */
public final class RespValue {
    private static final String CRLF = "\r\n";
    private static final byte[] CRLF_BYTES = CRLF.getBytes(StandardCharsets.UTF_8);
    private static final RespValue NULL_BULK_STRING = new RespValue(RespType.NULL_BULK_STRING, null, null, null);
    private static final RespValue OK = new RespValue(RespType.SIMPLE_STRING, "OK", null, null);
    private static final RespValue PONG = new RespValue(RespType.SIMPLE_STRING, "PONG", null, null);

    private final RespType type;
    private final String stringValue;
    private final Long integerValue;
    private final List<RespValue> arrayValue;

    private RespValue(RespType type, String stringValue, Long integerValue, List<RespValue> arrayValue) {
        this.type = type;
        this.stringValue = stringValue;
        this.integerValue = integerValue;
        this.arrayValue = arrayValue != null ? Collections.unmodifiableList(new ArrayList<>(arrayValue)) : null;
    }

    public static RespValue simpleString(String value) {
        if ("OK".equals(value)) return OK;
        if ("PONG".equals(value)) return PONG;
        return new RespValue(RespType.SIMPLE_STRING, Objects.requireNonNull(value), null, null);
    }

    public static RespValue error(String errorMessage) {
        return new RespValue(RespType.ERROR, Objects.requireNonNull(errorMessage), null, null);
    }

    public static RespValue integer(long value) {
        return new RespValue(RespType.INTEGER, null, value, null);
    }

    public static RespValue bulkString(String value) {
        if (value == null) {
            return NULL_BULK_STRING;
        }
        return new RespValue(RespType.BULK_STRING, value, null, null);
    }

    public static RespValue nullBulkString() {
        return NULL_BULK_STRING;
    }

    public static RespValue array(List<RespValue> elements) {
        if (elements == null) {
            return new RespValue(RespType.ARRAY, null, null, null);
        }
        return new RespValue(RespType.ARRAY, null, null, elements);
    }

    public RespType getType() {
        return type;
    }

    public String getStringValue() {
        return stringValue;
    }

    public Long getIntegerValue() {
        return integerValue;
    }

    public List<RespValue> getArrayValue() {
        return arrayValue;
    }

    public boolean isNull() {
        return type == RespType.NULL_BULK_STRING || (type == RespType.ARRAY && arrayValue == null);
    }

    public byte[] encode() {
        StringBuilder sb = new StringBuilder();
        switch (type) {
            case SIMPLE_STRING:
                sb.append('+').append(stringValue).append(CRLF);
                return sb.toString().getBytes(StandardCharsets.UTF_8);

            case ERROR:
                sb.append('-').append(stringValue).append(CRLF);
                return sb.toString().getBytes(StandardCharsets.UTF_8);

            case INTEGER:
                sb.append(':').append(integerValue).append(CRLF);
                return sb.toString().getBytes(StandardCharsets.UTF_8);

            case NULL_BULK_STRING:
                sb.append("$-1").append(CRLF);
                return sb.toString().getBytes(StandardCharsets.UTF_8);

            case BULK_STRING:
                byte[] bytes = stringValue.getBytes(StandardCharsets.UTF_8);
                sb.append('$').append(bytes.length).append(CRLF);
                byte[] header = sb.toString().getBytes(StandardCharsets.UTF_8);
                byte[] result = new byte[header.length + bytes.length + CRLF_BYTES.length];
                System.arraycopy(header, 0, result, 0, header.length);
                System.arraycopy(bytes, 0, result, header.length, bytes.length);
                System.arraycopy(CRLF_BYTES, 0, result, header.length + bytes.length, CRLF_BYTES.length);
                return result;

            case ARRAY:
                if (arrayValue == null) {
                    sb.append("*-1").append(CRLF);
                    return sb.toString().getBytes(StandardCharsets.UTF_8);
                }
                sb.append('*').append(arrayValue.size()).append(CRLF);
                byte[] arrayHeader = sb.toString().getBytes(StandardCharsets.UTF_8);
                List<byte[]> encodedElements = new ArrayList<>();
                int totalLen = arrayHeader.length;
                for (RespValue elem : arrayValue) {
                    byte[] elemBytes = elem.encode();
                    encodedElements.add(elemBytes);
                    totalLen += elemBytes.length;
                }
                byte[] combined = new byte[totalLen];
                System.arraycopy(arrayHeader, 0, combined, 0, arrayHeader.length);
                int offset = arrayHeader.length;
                for (byte[] elemBytes : encodedElements) {
                    System.arraycopy(elemBytes, 0, combined, offset, elemBytes.length);
                    offset += elemBytes.length;
                }
                return combined;

            default:
                throw new IllegalStateException("Unknown RESP type: " + type);
        }
    }

    @Override
    public String toString() {
        switch (type) {
            case SIMPLE_STRING:
                return "+" + stringValue;
            case ERROR:
                return "-" + stringValue;
            case INTEGER:
                return ":" + integerValue;
            case BULK_STRING:
                return "$" + stringValue;
            case NULL_BULK_STRING:
                return "$-1";
            case ARRAY:
                return "*" + (arrayValue != null ? arrayValue.toString() : "null");
            default:
                return super.toString();
        }
    }
}
