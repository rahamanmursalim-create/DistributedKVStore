package com.codecrafters.redis.protocol;

/**
 * RESP (REdis Serialization Protocol) data types.
 */
public enum RespType {
    SIMPLE_STRING('+'),
    ERROR('-'),
    INTEGER(':'),
    BULK_STRING('$'),
    ARRAY('*'),
    NULL_BULK_STRING('$');

    private final char prefix;

    RespType(char prefix) {
        this.prefix = prefix;
    }

    public char getPrefix() {
        return prefix;
    }
}
