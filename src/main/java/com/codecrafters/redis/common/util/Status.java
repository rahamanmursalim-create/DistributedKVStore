package com.distributedkv.common;

/**
 * Status enum for operation results
 */
public enum Status {
    OK(0, "OK"),
    UNKNOWN_ERROR(1, "Unknown error"),
    TIMEOUT(2, "Operation timed out"),
    NOT_LEADER(3, "Node is not the leader"),
    NOT_FOUND(4, "Key not found"),
    IO_ERROR(5, "I/O error"),
    NETWORK_ERROR(6, "Network error"),
    INVALID_ARGUMENT(7, "Invalid argument"),
    NOT_INITIALIZED(8, "Not initialized"),
    ALREADY_EXISTS(9, "Already exists"),
    NOT_SUPPORTED(10, "Not supported"),
    SERIALIZATION_ERROR(11, "Serialization error"),
    BUSY(12, "System is busy"),
    CANCELLED(13, "Operation cancelled"),
    CORRUPTION(14, "Data corruption detected"),
    TRY_AGAIN(15, "Try again later"),
    ABORTED(16, "Operation aborted"),
    OUT_OF_RANGE(17, "Value out of range"),
    UNAVAILABLE(18, "Service unavailable"),
    INTERNAL_ERROR(19, "Internal error"),
    PERMISSION_DENIED(20, "Permission denied"),
    UNAUTHENTICATED(21, "Unauthenticated request");

    private final int code;
    private final String message;

    Status(int code, String message) {
        this.code = code;
        this.message = message;
    }

    /**
     * Get the numeric code for this status
     *
     * @return the status code
     */
    public int getCode() {
        return code;
    }

    /**
     * Get the message for this status
     *
     * @return the status message
     */
    public String getMessage() {
        return message;
    }

    /**
     * Check if this status represents success
     *
     * @return true if status is OK
     */
    public boolean isOk() {
        return this == OK;
    }

    /**
     * Check if this status represents an error
     *
     * @return true if status is not OK
     */
    public boolean isError() {
        return !isOk();
    }

    /**
     * Get a status from its numeric code
     *
     * @param code the status code
     * @return the status enum value, or UNKNOWN_ERROR if not found
     */
    public static Status fromCode(int code) {
        for (Status status : values()) {
            if (status.code == code) {
                return status;
            }
        }
        return UNKNOWN_ERROR;
    }

    /**
     * Create a string representation with both code and message
     *
     * @return formatted status string
     */
    @Override
    public String toString() {
        return code + " [" + name() + "]: " + message;
    }
}