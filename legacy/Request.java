package com.distributedkv.network.protocol;

import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base class for request messages
 */
public abstract class Request extends Message {
    private static final long serialVersionUID = 1L;

    // Static counter for generating sequence numbers
    private static final AtomicInteger sequenceCounter = new AtomicInteger(0);

    // The request sequence number
    private final int sequenceNumber;

    // The request timeout in milliseconds
    private final long timeoutMs;

    // Additional headers for routing and processing
    private final Map<String, String> headers;

    // Future for waiting on the response
    private transient CompletableFuture<Response> responseFuture;

    /**
     * Create a new request
     *
     * @param type the request type
     * @param sourceNodeId the source node identifier
     * @param destNodeId the destination node identifier
     * @param groupId the group identifier
     * @param timeoutMs the request timeout in milliseconds
     */
    protected Request(Type type, String sourceNodeId, String destNodeId, String groupId, long timeoutMs) {
        super(type, sourceNodeId, destNodeId, groupId);
        this.sequenceNumber = sequenceCounter.incrementAndGet();
        this.timeoutMs = timeoutMs;
        this.headers = new HashMap<>();
        this.responseFuture = new CompletableFuture<>();
    }

    /**
     * Create a new request with a specified ID
     *
     * @param id the request identifier
     * @param type the request type
     * @param sourceNodeId the source node identifier
     * @param destNodeId the destination node identifier
     * @param groupId the group identifier
     * @param timeoutMs the request timeout in milliseconds
     */
    protected Request(String id, Type type, String sourceNodeId, String destNodeId, String groupId, long timeoutMs) {
        super(id, type, sourceNodeId, destNodeId, groupId);
        this.sequenceNumber = sequenceCounter.incrementAndGet();
        this.timeoutMs = timeoutMs;
        this.headers = new HashMap<>();
        this.responseFuture = new CompletableFuture<>();
    }

    /**
     * Get the request sequence number
     *
     * @return the sequence number
     */
    public int getSequenceNumber() {
        return sequenceNumber;
    }

    /**
     * Get the request timeout
     *
     * @return the timeout in milliseconds
     */
    public long getTimeoutMs() {
        return timeoutMs;
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
     * Add a header
     *
     * @param key the header key
     * @param value the header value
     * @return this request for chaining
     */
    public Request addHeader(String key, String value) {
        headers.put(key, value);
        return this;
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
     * Get the response future
     *
     * @return the response future
     */
    public CompletableFuture<Response> getResponseFuture() {
        if (responseFuture == null) {
            responseFuture = new CompletableFuture<>();
        }
        return responseFuture;
    }

    /**
     * Set the response for this request
     *
     * @param response the response
     */
    public void setResponse(Response response) {
        if (responseFuture != null && !responseFuture.isDone()) {
            responseFuture.complete(response);
        }
    }

    /**
     * Set an exception for this request
     *
     * @param exception the exception
     */
    public void setException(Throwable exception) {
        if (responseFuture != null && !responseFuture.isDone()) {
            responseFuture.completeExceptionally(exception);
        }
    }

    /**
     * Create a success response for this request
     *
     * @return a success response
     */
    public Response createSuccessResponse() {
        return new Response.Builder()
                .withType(Type.RESPONSE_SUCCESS)
                .withSourceNodeId(getDestNodeId())
                .withDestNodeId(getSourceNodeId())
                .withGroupId(getGroupId())
                .withRequestId(getId())
                .build();
    }

    /**
     * Create an error response for this request
     *
     * @param errorCode the error code
     * @param errorMessage the error message
     * @return an error response
     */
    public Response createErrorResponse(int errorCode, String errorMessage) {
        return new Response.Builder()
                .withType(Type.RESPONSE_ERROR)
                .withSourceNodeId(getDestNodeId())
                .withDestNodeId(getSourceNodeId())
                .withGroupId(getGroupId())
                .withRequestId(getId())
                .withErrorCode(errorCode)
                .withErrorMessage(errorMessage)
                .build();
    }

    /**
     * Builder class for creating requests
     */
    public static class Builder<T extends Builder<T>> {
        protected Type type;
        protected String sourceNodeId;
        protected String destNodeId;
        protected String groupId;
        protected long timeoutMs = 5000; // Default timeout: 5 seconds
        protected Map<String, String> headers = new HashMap<>();

        /**
         * Set the request type
         *
         * @param type the request type
         * @return this builder for chaining
         */
        @SuppressWarnings("unchecked")
        public T withType(Type type) {
            this.type = type;
            return (T) this;
        }

        /**
         * Set the source node identifier
         *
         * @param sourceNodeId the source node ID
         * @return this builder for chaining
         */
        @SuppressWarnings("unchecked")
        public T withSourceNodeId(String sourceNodeId) {
            this.sourceNodeId = sourceNodeId;
            return (T) this;
        }

        /**
         * Set the destination node identifier
         *
         * @param destNodeId the destination node ID
         * @return this builder for chaining
         */
        @SuppressWarnings("unchecked")
        public T withDestNodeId(String destNodeId) {
            this.destNodeId = destNodeId;
            return (T) this;
        }

        /**
         * Set the group identifier
         *
         * @param groupId the group ID
         * @return this builder for chaining
         */
        @SuppressWarnings("unchecked")
        public T withGroupId(String groupId) {
            this.groupId = groupId;
            return (T) this;
        }

        /**
         * Set the request timeout
         *
         * @param timeoutMs the timeout in milliseconds
         * @return this builder for chaining
         */
        @SuppressWarnings("unchecked")
        public T withTimeoutMs(long timeoutMs) {
            this.timeoutMs = timeoutMs;
            return (T) this;
        }

        /**
         * Add a header
         *
         * @param key the header key
         * @param value the header value
         * @return this builder for chaining
         */
        @SuppressWarnings("unchecked")
        public T withHeader(String key, String value) {
            this.headers.put(key, value);
            return (T) this;
        }
    }
}