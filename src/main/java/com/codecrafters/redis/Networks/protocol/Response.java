package com.distributedkv.network.protocol;

import com.distributedkv.common.Status;

import java.util.HashMap;
import java.util.Map;

/**
 * Base class for response messages
 */
public class Response extends Message {
    private static final long serialVersionUID = 1L;

    // The original request ID
    private final String requestId;

    // Error code (0 means success)
    private final int errorCode;

    // Error message (null if success)
    private final String errorMessage;

    // Additional headers for routing and processing
    private final Map<String, String> headers;

    /**
     * Private constructor used by the builder
     */
    private Response(Builder builder) {
        super(builder.type, builder.sourceNodeId, builder.destNodeId, builder.groupId);
        this.requestId = builder.requestId;
        this.errorCode = builder.errorCode;
        this.errorMessage = builder.errorMessage;
        this.headers = new HashMap<>(builder.headers);
    }

    /**
     * Get the original request ID
     *
     * @return the request ID
     */
    public String getRequestId() {
        return requestId;
    }

    /**
     * Get the error code
     *
     * @return the error code (0 means success)
     */
    public int getErrorCode() {
        return errorCode;
    }

    /**
     * Get the error message
     *
     * @return the error message (null if success)
     */
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * Check if the response is successful
     *
     * @return true if the response is successful
     */
    public boolean isSuccess() {
        return errorCode == 0;
    }

    /**
     * Get the headers map
     *
     * @return the headers map
     */
    public Map<String, String> getHeaders() {
        return headers;
    }

    /**
     * Get a header value
     *
     * @param key the header key
     * @return the header value or null if not found
     */
    public String getHeader(String key) {
        return headers.get(key);
    }

    /**
     * Convert this response to a Result
     *
     * @param <T> the result data type
     * @param data the result data
     * @return a Result object
     */
    public <T> com.distributedkv.common.Result<T> toResult(T data) {
        if (isSuccess()) {
            return com.distributedkv.common.Result.success(data);
        } else {
            Status status = Status.fromCode(errorCode);
            if (status == Status.UNKNOWN_ERROR && errorCode != 0) {
                // Use the error message as is for unknown error codes
                return com.distributedkv.common.Result.fail(status, errorMessage);
            } else {
                return com.distributedkv.common.Result.fail(status, errorMessage);
            }
        }
    }

    /**
     * Convert this response to a Result with no data
     *
     * @param <T> the result data type
     * @return a Result object
     */
    public <T> com.distributedkv.common.Result<T> toResult() {
        if (isSuccess()) {
            return com.distributedkv.common.Result.success();
        } else {
            Status status = Status.fromCode(errorCode);
            if (status == Status.UNKNOWN_ERROR && errorCode != 0) {
                // Use the error message as is for unknown error codes
                return com.distributedkv.common.Result.fail(status, errorMessage);
            } else {
                return com.distributedkv.common.Result.fail(status, errorMessage);
            }
        }
    }

    @Override
    public String toString() {
        return "Response{" +
                "id='" + getId() + '\'' +
                ", type=" + getType() +
                ", requestId='" + requestId + '\'' +
                ", errorCode=" + errorCode +
                ", errorMessage='" + errorMessage + '\'' +
                ", sourceNodeId='" + getSourceNodeId() + '\'' +
                ", destNodeId='" + getDestNodeId() + '\'' +
                ", groupId='" + getGroupId() + '\'' +
                '}';
    }

    /**
     * Builder class for creating responses
     */
    public static class Builder {
        protected Type type;
        protected String sourceNodeId;
        protected String destNodeId;
        protected String groupId;
        protected String requestId;
        protected int errorCode = 0;
        protected String errorMessage = null;
        protected Map<String, String> headers = new HashMap<>();

        /**
         * Set the response type
         *
         * @param type the response type
         * @return this builder for chaining
         */
        public Builder withType(Type type) {
            this.type = type;
            return this;
        }

        /**
         * Set the source node identifier
         *
         * @param sourceNodeId the source node ID
         * @return this builder for chaining
         */
        public Builder withSourceNodeId(String sourceNodeId) {
            this.sourceNodeId = sourceNodeId;
            return this;
        }

        /**
         * Set the destination node identifier
         *
         * @param destNodeId the destination node ID
         * @return this builder for chaining
         */
        public Builder withDestNodeId(String destNodeId) {
            this.destNodeId = destNodeId;
            return this;
        }

        /**
         * Set the group identifier
         *
         * @param groupId the group ID
         * @return this builder for chaining
         */
        public Builder withGroupId(String groupId) {
            this.groupId = groupId;
            return this;
        }

        /**
         * Set the original request ID
         *
         * @param requestId the request ID
         * @return this builder for chaining
         */
        public Builder withRequestId(String requestId) {
            this.requestId = requestId;
            return this;
        }

        /**
         * Set the error code
         *
         * @param errorCode the error code
         * @return this builder for chaining
         */
        public Builder withErrorCode(int errorCode) {
            this.errorCode = errorCode;
            return this;
        }

        /**
         * Set the error message
         *
         * @param errorMessage the error message
         * @return this builder for chaining
         */
        public Builder withErrorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
            return this;
        }

        /**
         * Set the error from a Status
         *
         * @param status the status
         * @param message optional additional message
         * @return this builder for chaining
         */
        public Builder withError(Status status, String message) {
            this.errorCode = status.getCode();
            this.errorMessage = message != null ? message : status.getMessage();
            return this;
        }

        /**
         * Add a header
         *
         * @param key the header key
         * @param value the header value
         * @return this builder for chaining
         */
        public Builder withHeader(String key, String value) {
            this.headers.put(key, value);
            return this;
        }

        /**
         * Build the response
         *
         * @return a new Response object
         */
        public Response build() {
            if (type == null) {
                throw new IllegalStateException("Response type must be set");
            }
            if (sourceNodeId == null) {
                throw new IllegalStateException("Source node ID must be set");
            }
            if (destNodeId == null) {
                throw new IllegalStateException("Destination node ID must be set");
            }
            if (requestId == null) {
                throw new IllegalStateException("Request ID must be set");
            }

            return new Response(this);
        }
    }
}