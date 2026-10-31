package com.distributedkv.network.protocol;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

/**
 * Messages for Key-Value operations
 */
public class KVMessage {
    /**
     * GET request message
     */
    public static class Get extends Request {
        private static final long serialVersionUID = 1L;

        // The key to get
        private final byte[] key;

        // Whether to allow reading from followers
        private final boolean allowFollowerRead;

        /**
         * Create a new GET request
         *
         * @param sourceNodeId the source node identifier
         * @param destNodeId the destination node identifier
         * @param groupId the group identifier
         * @param timeoutMs the request timeout in milliseconds
         * @param key the key to get
         * @param allowFollowerRead whether to allow reading from followers
         */
        public Get(String sourceNodeId, String destNodeId, String groupId, long timeoutMs, byte[] key, boolean allowFollowerRead) {
            super(Type.REQUEST_KV_GET, sourceNodeId, destNodeId, groupId, timeoutMs);
            this.key = key;
            this.allowFollowerRead = allowFollowerRead;
        }

        /**
         * Get the key
         *
         * @return the key
         */
        public byte[] getKey() {
            return key;
        }

        /**
         * Check if reading from followers is allowed
         *
         * @return true if reading from followers is allowed
         */
        public boolean isAllowFollowerRead() {
            return allowFollowerRead;
        }

        /**
         * Create a response for this request
         *
         * @param value the value found
         * @return a response message
         */
        public Response createResponse(byte[] value) {
            GetResponse response = new GetResponse(getDestNodeId(), getSourceNodeId(), getGroupId(), getId(), value);
            return response;
        }

        /**
         * Builder for Get requests
         */
        public static class Builder extends Request.Builder<Builder> {
            private byte[] key;
            private boolean allowFollowerRead = false;

            /**
             * Set the key
             *
             * @param key the key to get
             * @return this builder for chaining
             */
            public Builder withKey(byte[] key) {
                this.key = key;
                return this;
            }

            /**
             * Set whether to allow reading from followers
             *
             * @param allowFollowerRead true to allow reading from followers
             * @return this builder for chaining
             */
            public Builder withAllowFollowerRead(boolean allowFollowerRead) {
                this.allowFollowerRead = allowFollowerRead;
                return this;
            }

            /**
             * Build the request
             *
             * @return a new Get request
             */
            public Get build() {
                if (type == null) {
                    type = Type.REQUEST_KV_GET;
                }
                if (key == null) {
                    throw new IllegalStateException("Key must be set");
                }
                if (sourceNodeId == null) {
                    throw new IllegalStateException("Source node ID must be set");
                }
                if (destNodeId == null) {
                    throw new IllegalStateException("Destination node ID must be set");
                }

                Get request = new Get(sourceNodeId, destNodeId, groupId, timeoutMs, key, allowFollowerRead);

                // Add headers
                headers.forEach(request::addHeader);

                return request;
            }
        }
    }

    /**
     * GET response message
     */
    public static class GetResponse extends Response {
        private static final long serialVersionUID = 1L;

        // The value found, or null if not found
        private final byte[] value;

        /**
         * Create a new GET response
         *
         * @param sourceNodeId the source node identifier
         * @param destNodeId the destination node identifier
         * @param groupId the group identifier
         * @param requestId the original request ID
         * @param value the value found, or null if not found
         */
        public GetResponse(String sourceNodeId, String destNodeId, String groupId, String requestId, byte[] value) {
            super(new Response.Builder()
                    .withType(Type.RESPONSE_KV_GET)
                    .withSourceNodeId(sourceNodeId)
                    .withDestNodeId(destNodeId)
                    .withGroupId(groupId)
                    .withRequestId(requestId)
                    .withErrorCode(value == null ? 4 : 0) // 4 = NOT_FOUND
                    .withErrorMessage(value == null ? "Key not found" : null));

            this.value = value;
        }

        /**
         * Get the value
         *
         * @return the value, or null if not found
         */
        public byte[] getValue() {
            return value;
        }
    }

    /**
     * PUT request message
     */
    public static class Put extends Request {
        private static final long serialVersionUID = 1L;

        // The key to put
        private final byte[] key;

        // The value to put
        private final byte[] value;

        // Whether to sync the write to disk
        private final boolean sync;

        /**
         * Create a new PUT request
         *
         * @param sourceNodeId the source node identifier
         * @param destNodeId the destination node identifier
         * @param groupId the group identifier
         * @param timeoutMs the request timeout in milliseconds
         * @param key the key to put
         * @param value the value to put
         * @param sync whether to sync the write to disk
         */
        public Put(String sourceNodeId, String destNodeId, String groupId, long timeoutMs, byte[] key, byte[] value, boolean sync) {
            super(Type.REQUEST_KV_PUT, sourceNodeId, destNodeId, groupId, timeoutMs);
            this.key = key;
            this.value = value;
            this.sync = sync;
        }

        /**
         * Get the key
         *
         * @return the key
         */
        public byte[] getKey() {
            return key;
        }

        /**
         * Get the value
         *
         * @return the value
         */
        public byte[] getValue() {
            return value;
        }

        /**
         * Check if the write should be synced to disk
         *
         * @return true if the write should be synced to disk
         */
        public boolean isSync() {
            return sync;
        }

        /**
         * Builder for Put requests
         */
        public static class Builder extends Request.Builder<Builder> {
            private byte[] key;
            private byte[] value;
            private boolean sync = true;

            /**
             * Set the key
             *
             * @param key the key to put
             * @return this builder for chaining
             */
            public Builder withKey(byte[] key) {
                this.key = key;
                return this;
            }

            /**
             * Set the value
             *
             * @param value the value to put
             * @return this builder for chaining
             */
            public Builder withValue(byte[] value) {
                this.value = value;
                return this;
            }

            /**
             * Set whether to sync the write to disk
             *
             * @param sync true to sync the write to disk
             * @return this builder for chaining
             */
            public Builder withSync(boolean sync) {
                this.sync = sync;
                return this;
            }

            /**
             * Build the request
             *
             * @return a new Put request
             */
            public Put build() {
                if (type == null) {
                    type = Type.REQUEST_KV_PUT;
                }
                if (key == null) {
                    throw new IllegalStateException("Key must be set");
                }
                if (value == null) {
                    throw new IllegalStateException("Value must be set");
                }
                if (sourceNodeId == null) {
                    throw new IllegalStateException("Source node ID must be set");
                }
                if (destNodeId == null) {
                    throw new IllegalStateException("Destination node ID must be set");
                }

                Put request = new Put(sourceNodeId, destNodeId, groupId, timeoutMs, key, value, sync);

                // Add headers
                headers.forEach(request::addHeader);

                return request;
            }
        }
    }

    /**
     * DELETE request message
     */
    public static class Delete extends Request {
        private static final long serialVersionUID = 1L;

        // The key to delete
        private final byte[] key;

        // Whether to sync the delete to disk
        private final boolean sync;

        /**
         * Create a new DELETE request
         *
         * @param sourceNodeId the source node identifier
         * @param destNodeId the destination node identifier
         * @param groupId the group identifier
         * @param timeoutMs the request timeout in milliseconds
         * @param key the key to delete
         * @param sync whether to sync the delete to disk
         */
        public Delete(String sourceNodeId, String destNodeId, String groupId, long timeoutMs, byte[] key, boolean sync) {
            super(Type.REQUEST_KV_DELETE, sourceNodeId, destNodeId, groupId, timeoutMs);
            this.key = key;
            this.sync = sync;
        }

        /**
         * Get the key
         *
         * @return the key
         */
        public byte[] getKey() {
            return key;
        }

        /**
         * Check if the delete should be synced to disk
         *
         * @return true if the delete should be synced to disk
         */
        public boolean isSync() {
            return sync;
        }

        /**
         * Builder for Delete requests
         */
        public static class Builder extends Request.Builder<Builder> {
            private byte[] key;
            private boolean sync = true;

            /**
             * Set the key
             *
             * @param key the key to delete
             * @return this builder for chaining
             */
            public Builder withKey(byte[] key) {
                this.key = key;
                return this;
            }

            /**
             * Set whether to sync the delete to disk
             *
             * @param sync true to sync the delete to disk
             * @return this builder for chaining
             */
            public Builder withSync(boolean sync) {
                this.sync = sync;
                return this;
            }

            /**
             * Build the request
             *
             * @return a new Delete request
             */
            public Delete build() {
                if (type == null) {
                    type = Type.REQUEST_KV_DELETE;
                }
                if (key == null) {
                    throw new IllegalStateException("Key must be set");
                }
                if (sourceNodeId == null) {
                    throw new IllegalStateException("Source node ID must be set");
                }
                if (destNodeId == null) {
                    throw new IllegalStateException("Destination node ID must be set");
                }

                Delete request = new Delete(sourceNodeId, destNodeId, groupId, timeoutMs, key, sync);

                // Add headers
                headers.forEach(request::addHeader);

                return request;
            }
        }
    }

    /**
     * SCAN request message
     */
    public static class Scan extends Request {
        private static final long serialVersionUID = 1L;

        // The start key (inclusive)
        private final byte[] startKey;

        // The end key (exclusive)
        private final byte[] endKey;

        // The maximum number of results to return
        private final int limit;

        /**
         * Create a new SCAN request
         *
         * @param sourceNodeId the source node identifier
         * @param destNodeId the destination node identifier
         * @param groupId the group identifier
         * @param timeoutMs the request timeout in milliseconds
         * @param startKey the start key (inclusive)
         * @param endKey the end key (exclusive)
         * @param limit the maximum number of results to return
         */
        public Scan(String sourceNodeId, String destNodeId, String groupId, long timeoutMs,
                    byte[] startKey, byte[] endKey, int limit) {
            super(Type.REQUEST_KV_SCAN, sourceNodeId, destNodeId, groupId, timeoutMs);
            this.startKey = startKey;
            this.endKey = endKey;
            this.limit = limit;
        }

        /**
         * Get the start key
         *
         * @return the start key
         */
        public byte[] getStartKey() {
            return startKey;
        }

        /**
         * Get the end key
         *
         * @return the end key
         */
        public byte[] getEndKey() {
            return endKey;
        }

        /**
         * Get the limit
         *
         * @return the maximum number of results to return
         */
        public int getLimit() {
            return limit;
        }

        /**
         * Create a response for this request
         *
         * @param keyValues the key-value pairs found
         * @return a response message
         */
        public Response createResponse(List<KeyValue> keyValues) {
            ScanResponse response = new ScanResponse(getDestNodeId(), getSourceNodeId(), getGroupId(), getId(), keyValues);
            return response;
        }

        /**
         * Builder for Scan requests
         */
        public static class Builder extends Request.Builder<Builder> {
            private byte[] startKey;
            private byte[] endKey;
            private int limit = 100; // Default limit

            /**
             * Set the start key
             *
             * @param startKey the start key (inclusive)
             * @return this builder for chaining
             */
            public Builder withStartKey(byte[] startKey) {
                this.startKey = startKey;
                return this;
            }

            /**
             * Set the end key
             *
             * @param endKey the end key (exclusive)
             * @return this builder for chaining
             */
            public Builder withEndKey(byte[] endKey) {
                this.endKey = endKey;
                return this;
            }

            /**
             * Set the limit
             *
             * @param limit the maximum number of results to return
             * @return this builder for chaining
             */
            public Builder withLimit(int limit) {
                if (limit <= 0) {
                    throw new IllegalArgumentException("Limit must be positive");
                }
                this.limit = limit;
                return this;
            }

            /**
             * Build the request
             *
             * @return a new Scan request
             */
            public Scan build() {
                if (type == null) {
                    type = Type.REQUEST_KV_SCAN;
                }
                if (startKey == null) {
                    throw new IllegalStateException("Start key must be set");
                }
                if (sourceNodeId == null) {
                    throw new IllegalStateException("Source node ID must be set");
                }
                if (destNodeId == null) {
                    throw new IllegalStateException("Destination node ID must be set");
                }

                Scan request = new Scan(sourceNodeId, destNodeId, groupId, timeoutMs, startKey, endKey, limit);

                // Add headers
                headers.forEach(request::addHeader);

                return request;
            }
        }
    }

    /**
     * Key-Value pair for scan results
     */
    public static class KeyValue implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private final byte[] key;
        private final byte[] value;

        /**
         * Create a new KeyValue pair
         *
         * @param key the key
         * @param value the value
         */
        public KeyValue(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }

        /**
         * Get the key
         *
         * @return the key
         */
        public byte[] getKey() {
            return key;
        }

        /**
         * Get the value
         *
         * @return the value
         */
        public byte[] getValue() {
            return value;
        }
    }

    /**
     * SCAN response message
     */
    public static class ScanResponse extends Response {
        private static final long serialVersionUID = 1L;

        // The key-value pairs found
        private final List<KeyValue> keyValues;

        /**
         * Create a new SCAN response
         *
         * @param sourceNodeId the source node identifier
         * @param destNodeId the destination node identifier
         * @param groupId the group identifier
         * @param requestId the original request ID
         * @param keyValues the key-value pairs found
         */
        public ScanResponse(String sourceNodeId, String destNodeId, String groupId, String requestId, List<KeyValue> keyValues) {
            super(new Response.Builder()
                    .withType(Type.RESPONSE_KV_SCAN)
                    .withSourceNodeId(sourceNodeId)
                    .withDestNodeId(destNodeId)
                    .withGroupId(groupId)
                    .withRequestId(requestId));

            this.keyValues = keyValues != null ? new ArrayList<>(keyValues) : new ArrayList<>();
        }

        /**
         * Get the key-value pairs
         *
         * @return the key-value pairs
         */
        public List<KeyValue> getKeyValues() {
            return new ArrayList<>(keyValues);
        }
    }

    /**
     * BATCH request message
     */
    public static class Batch extends Request {
        private static final long serialVersionUID = 1L;

        /**
         * Types of batch operations
         */
        public enum OperationType {
            PUT,
            DELETE
        }

        /**
         * A single operation in a batch
         */
        public static class Operation implements java.io.Serializable {
            private static final long serialVersionUID = 1L;

            private final OperationType type;
            private final byte[] key;
            private final byte[] value; // Only used for PUT

            /**
             * Create a new operation
             *
             * @param type the operation type
             * @param key the key
             * @param value the value (only for PUT)
             */
            public Operation(OperationType type, byte[] key, byte[] value) {
                this.type = type;
                this.key = key;
                this.value = value;
            }

            /**
             * Get the operation type
             *
             * @return the operation type
             */
            public OperationType getType() {
                return type;
            }

            /**
             * Get the key
             *
             * @return the key
             */
            public byte[] getKey() {
                return key;
            }

            /**
             * Get the value
             *
             * @return the value
             */
            public byte[] getValue() {
                return value;
            }
        }

        // The batch operations
        private final List<Operation> operations;

        // Whether to sync the operations to disk
        private final boolean sync;

        /**
         * Create a new BATCH request
         *
         * @param sourceNodeId the source node identifier
         * @param destNodeId the destination node identifier
         * @param groupId the group identifier
         * @param timeoutMs the request timeout in milliseconds
         * @param operations the batch operations
         * @param sync whether to sync the operations to disk
         */
        public Batch(String sourceNodeId, String destNodeId, String groupId, long timeoutMs,
                    List<Operation> operations, boolean sync) {
            super(Type.REQUEST_KV_BATCH, sourceNodeId, destNodeId, groupId, timeoutMs);
            this.operations = new ArrayList<>(operations);
            this.sync = sync;
        }

        /**
         * Get the operations
         *
         * @return the batch operations
         */
        public List<Operation> getOperations() {
            return new ArrayList<>(operations);
        }

        /**
         * Check if the operations should be synced to disk
         *
         * @return true if the operations should be synced to disk
         */
        public boolean isSync() {
            return sync;
        }

        /**
         * Builder for Batch requests
         */
        public static class Builder extends Request.Builder<Builder> {
            private final List<Operation> operations = new ArrayList<>();
            private boolean sync = true;

            /**
             * Add a PUT operation
             *
             * @param key the key to put
             * @param value the value to put
             * @return this builder for chaining
             */
            public Builder addPut(byte[] key, byte[] value) {
                operations.add(new Operation(OperationType.PUT, key, value));
                return this;
            }

            /**
             * Add a DELETE operation
             *
             * @param key the key to delete
             * @return this builder for chaining
             */
            public Builder addDelete(byte[] key) {
                operations.add(new Operation(OperationType.DELETE, key, null));
                return this;
            }

            /**
             * Set whether to sync the operations to disk
             *
             * @param sync true to sync the operations to disk
             * @return this builder for chaining
             */
            public Builder withSync(boolean sync) {
                this.sync = sync;
                return this;
            }

            /**
             * Build the request
             *
             * @return a new Batch request
             */
            public Batch build() {
                if (type == null) {
                    type = Type.REQUEST_KV_BATCH;
                }
                if (operations.isEmpty()) {
                    throw new IllegalStateException("No operations added to batch");
                }
                if (sourceNodeId == null) {
                    throw new IllegalStateException("Source node ID must be set");
                }
                if (destNodeId == null) {
                    throw new IllegalStateException("Destination node ID must be set");
                }

                Batch request = new Batch(sourceNodeId, destNodeId, groupId, timeoutMs, operations, sync);

                // Add headers
                headers.forEach(request::addHeader);

                return request;
            }
        }
    }
}