package com.distributedkv.network.protocol.codec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.distributedkv.common.Result;
import com.distributedkv.common.Status;
import com.distributedkv.network.protocol.KVMessage;
import com.distributedkv.network.protocol.Message;
import com.distributedkv.network.protocol.RaftMessage;
import com.distributedkv.network.protocol.Request;
import com.distributedkv.network.protocol.Response;
import com.distributedkv.raft.LogEntry;
import com.distributedkv.raft.Snapshot;
import com.google.protobuf.ByteString;

/**
 * Protocol buffer based message codec implementation.
 * Handles serialization and deserialization of network messages using Protocol Buffers.
 */
public class ProtobufCodec implements MessageCodec {

    // Type registry for message types - maps protobuf message types to Java types
    private static final Map<Integer, Class<?>> TYPE_REGISTRY = new HashMap<>();

    // Initialize the type registry
    static {
        // Raft messages
        TYPE_REGISTRY.put(1, RaftMessage.RequestVote.class);
        TYPE_REGISTRY.put(2, RaftMessage.RequestVoteResponse.class);
        TYPE_REGISTRY.put(3, RaftMessage.AppendEntries.class);
        TYPE_REGISTRY.put(4, RaftMessage.AppendEntriesResponse.class);
        TYPE_REGISTRY.put(5, RaftMessage.InstallSnapshot.class);
        TYPE_REGISTRY.put(6, RaftMessage.InstallSnapshotResponse.class);
        TYPE_REGISTRY.put(7, RaftMessage.LeaderInfoRequest.class);
        TYPE_REGISTRY.put(8, RaftMessage.LeaderInfoResponse.class);
        TYPE_REGISTRY.put(9, RaftMessage.ReadIndexRequest.class);
        TYPE_REGISTRY.put(10, RaftMessage.ReadIndexResponse.class);

        // KV messages
        TYPE_REGISTRY.put(11, KVMessage.GetRequest.class);
        TYPE_REGISTRY.put(12, KVMessage.GetResponse.class);
        TYPE_REGISTRY.put(13, KVMessage.PutRequest.class);
        TYPE_REGISTRY.put(14, KVMessage.PutResponse.class);
        TYPE_REGISTRY.put(15, KVMessage.DeleteRequest.class);
        TYPE_REGISTRY.put(16, KVMessage.DeleteResponse.class);
        TYPE_REGISTRY.put(17, KVMessage.ScanRequest.class);
        TYPE_REGISTRY.put(18, KVMessage.ScanResponse.class);
    }

    @Override
    public byte[] encode(Message message) throws IOException {
        if (message == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }

        // Create the protobuf message
        MessageProto.Message.Builder builder = MessageProto.Message.newBuilder();

        // Set message type
        int messageType = getMessageType(message.getClass());
        builder.setType(messageType);

        // Set message content based on type
        if (message instanceof RaftMessage.RequestVote) {
            builder.setContent(encodeRequestVote((RaftMessage.RequestVote) message));
        } else if (message instanceof RaftMessage.RequestVoteResponse) {
            builder.setContent(encodeRequestVoteResponse((RaftMessage.RequestVoteResponse) message));
        } else if (message instanceof RaftMessage.AppendEntries) {
            builder.setContent(encodeAppendEntries((RaftMessage.AppendEntries) message));
        } else if (message instanceof RaftMessage.AppendEntriesResponse) {
            builder.setContent(encodeAppendEntriesResponse((RaftMessage.AppendEntriesResponse) message));
        } else if (message instanceof RaftMessage.InstallSnapshot) {
            builder.setContent(encodeInstallSnapshot((RaftMessage.InstallSnapshot) message));
        } else if (message instanceof RaftMessage.InstallSnapshotResponse) {
            builder.setContent(encodeInstallSnapshotResponse((RaftMessage.InstallSnapshotResponse) message));
        } else if (message instanceof RaftMessage.LeaderInfoRequest) {
            builder.setContent(encodeLeaderInfoRequest((RaftMessage.LeaderInfoRequest) message));
        } else if (message instanceof RaftMessage.LeaderInfoResponse) {
            builder.setContent(encodeLeaderInfoResponse((RaftMessage.LeaderInfoResponse) message));
        } else if (message instanceof RaftMessage.ReadIndexRequest) {
            builder.setContent(encodeReadIndexRequest((RaftMessage.ReadIndexRequest) message));
        } else if (message instanceof RaftMessage.ReadIndexResponse) {
            builder.setContent(encodeReadIndexResponse((RaftMessage.ReadIndexResponse) message));
        } else if (message instanceof KVMessage.GetRequest) {
            builder.setContent(encodeGetRequest((KVMessage.GetRequest) message));
        } else if (message instanceof KVMessage.GetResponse) {
            builder.setContent(encodeGetResponse((KVMessage.GetResponse) message));
        } else if (message instanceof KVMessage.PutRequest) {
            builder.setContent(encodePutRequest((KVMessage.PutRequest) message));
        } else if (message instanceof KVMessage.PutResponse) {
            builder.setContent(encodePutResponse((KVMessage.PutResponse) message));
        } else if (message instanceof KVMessage.DeleteRequest) {
            builder.setContent(encodeDeleteRequest((KVMessage.DeleteRequest) message));
        } else if (message instanceof KVMessage.DeleteResponse) {
            builder.setContent(encodeDeleteResponse((KVMessage.DeleteResponse) message));
        } else if (message instanceof KVMessage.ScanRequest) {
            builder.setContent(encodeScanRequest((KVMessage.ScanRequest) message));
        } else if (message instanceof KVMessage.ScanResponse) {
            builder.setContent(encodeScanResponse((KVMessage.ScanResponse) message));
        } else {
            throw new IllegalArgumentException("Unsupported message type: " + message.getClass().getName());
        }

        // Build and return the serialized message
        MessageProto.Message protoMessage = builder.build();
        return protoMessage.toByteArray();
    }

    @Override
    public Message decode(byte[] data) throws IOException {
        if (data == null || data.length == 0) {
            throw new IllegalArgumentException("Data cannot be null or empty");
        }

        try {
            // Parse the protobuf message
            MessageProto.Message protoMessage = MessageProto.Message.parseFrom(data);

            // Get the message type
            int messageType = protoMessage.getType();
            Class<?> messageClass = TYPE_REGISTRY.get(messageType);

            if (messageClass == null) {
                throw new IllegalArgumentException("Unknown message type: " + messageType);
            }

            // Decode the message content based on type
            ByteString content = protoMessage.getContent();

            if (messageClass == RaftMessage.RequestVote.class) {
                return decodeRequestVote(content);
            } else if (messageClass == RaftMessage.RequestVoteResponse.class) {
                return decodeRequestVoteResponse(content);
            } else if (messageClass == RaftMessage.AppendEntries.class) {
                return decodeAppendEntries(content);
            } else if (messageClass == RaftMessage.AppendEntriesResponse.class) {
                return decodeAppendEntriesResponse(content);
            } else if (messageClass == RaftMessage.InstallSnapshot.class) {
                return decodeInstallSnapshot(content);
            } else if (messageClass == RaftMessage.InstallSnapshotResponse.class) {
                return decodeInstallSnapshotResponse(content);
            } else if (messageClass == RaftMessage.LeaderInfoRequest.class) {
                return decodeLeaderInfoRequest(content);
            } else if (messageClass == RaftMessage.LeaderInfoResponse.class) {
                return decodeLeaderInfoResponse(content);
            } else if (messageClass == RaftMessage.ReadIndexRequest.class) {
                return decodeReadIndexRequest(content);
            } else if (messageClass == RaftMessage.ReadIndexResponse.class) {
                return decodeReadIndexResponse(content);
            } else if (messageClass == KVMessage.GetRequest.class) {
                return decodeGetRequest(content);
            } else if (messageClass == KVMessage.GetResponse.class) {
                return decodeGetResponse(content);
            } else if (messageClass == KVMessage.PutRequest.class) {
                return decodePutRequest(content);
            } else if (messageClass == KVMessage.PutResponse.class) {
                return decodePutResponse(content);
            } else if (messageClass == KVMessage.DeleteRequest.class) {
                return decodeDeleteRequest(content);
            } else if (messageClass == KVMessage.DeleteResponse.class) {
                return decodeDeleteResponse(content);
            } else if (messageClass == KVMessage.ScanRequest.class) {
                return decodeScanRequest(content);
            } else if (messageClass == KVMessage.ScanResponse.class) {
                return decodeScanResponse(content);
            } else {
                throw new IllegalArgumentException("Unsupported message class: " + messageClass.getName());
            }
        } catch (Exception e) {
            throw new IOException("Failed to decode message", e);
        }
    }

    // Helper methods for encoding/decoding specific message types

    // Raft message encoding methods

    private ByteString encodeRequestVote(RaftMessage.RequestVote message) {
        MessageProto.RequestVote.Builder builder = MessageProto.RequestVote.newBuilder();
        builder.setTerm(message.getTerm());
        builder.setCandidateId(message.getCandidateId());
        builder.setLastLogIndex(message.getLastLogIndex());
        builder.setLastLogTerm(message.getLastLogTerm());
        return builder.build().toByteString();
    }

    private RaftMessage.RequestVote decodeRequestVote(ByteString data) throws IOException {
        MessageProto.RequestVote protoMessage = MessageProto.RequestVote.parseFrom(data);
        return new RaftMessage.RequestVote(
                protoMessage.getTerm(),
                protoMessage.getCandidateId(),
                protoMessage.getLastLogIndex(),
                protoMessage.getLastLogTerm()
        );
    }

    private ByteString encodeRequestVoteResponse(RaftMessage.RequestVoteResponse message) {
        MessageProto.RequestVoteResponse.Builder builder = MessageProto.RequestVoteResponse.newBuilder();
        builder.setTerm(message.getTerm());
        builder.setVoteGranted(message.isVoteGranted());
        return builder.build().toByteString();
    }

    private RaftMessage.RequestVoteResponse decodeRequestVoteResponse(ByteString data) throws IOException {
        MessageProto.RequestVoteResponse protoMessage = MessageProto.RequestVoteResponse.parseFrom(data);
        return new RaftMessage.RequestVoteResponse(
                protoMessage.getTerm(),
                protoMessage.getVoteGranted()
        );
    }

    private ByteString encodeAppendEntries(RaftMessage.AppendEntries message) {
        MessageProto.AppendEntries.Builder builder = MessageProto.AppendEntries.newBuilder();
        builder.setTerm(message.getTerm());
        builder.setLeaderId(message.getLeaderId());
        builder.setPrevLogIndex(message.getPrevLogIndex());
        builder.setPrevLogTerm(message.getPrevLogTerm());
        builder.setLeaderCommit(message.getLeaderCommit());

        // Encode log entries
        LogEntry[] entries = message.getEntries();
        if (entries != null) {
            for (LogEntry entry : entries) {
                MessageProto.LogEntry.Builder entryBuilder = MessageProto.LogEntry.newBuilder();
                entryBuilder.setTerm(entry.getTerm());
                entryBuilder.setIndex(entry.getIndex());
                if (entry.getData() != null) {
                    entryBuilder.setData(ByteString.copyFrom(entry.getData()));
                }
                builder.addEntries(entryBuilder.build());
            }
        }

        return builder.build().toByteString();
    }

    private RaftMessage.AppendEntries decodeAppendEntries(ByteString data) throws IOException {
        MessageProto.AppendEntries protoMessage = MessageProto.AppendEntries.parseFrom(data);

        // Decode log entries
        LogEntry[] entries = new LogEntry[protoMessage.getEntriesCount()];
        for (int i = 0; i < entries.length; i++) {
            MessageProto.LogEntry protoEntry = protoMessage.getEntries(i);
            entries[i] = new LogEntry(
                    protoEntry.getTerm(),
                    protoEntry.getIndex(),
                    protoEntry.getData().toByteArray()
            );
        }

        return new RaftMessage.AppendEntries(
                protoMessage.getTerm(),
                protoMessage.getLeaderId(),
                protoMessage.getPrevLogIndex(),
                protoMessage.getPrevLogTerm(),
                entries,
                protoMessage.getLeaderCommit()
        );
    }

    private ByteString encodeAppendEntriesResponse(RaftMessage.AppendEntriesResponse message) {
        MessageProto.AppendEntriesResponse.Builder builder = MessageProto.AppendEntriesResponse.newBuilder();
        builder.setTerm(message.getTerm());
        builder.setSuccess(message.isSuccess());
        builder.setLastLogIndex(message.getLastLogIndex());
        return builder.build().toByteString();
    }

    private RaftMessage.AppendEntriesResponse decodeAppendEntriesResponse(ByteString data) throws IOException {
        MessageProto.AppendEntriesResponse protoMessage = MessageProto.AppendEntriesResponse.parseFrom(data);
        return new RaftMessage.AppendEntriesResponse(
                protoMessage.getTerm(),
                protoMessage.getSuccess(),
                protoMessage.getLastLogIndex()
        );
    }

    private ByteString encodeInstallSnapshot(RaftMessage.InstallSnapshot message) {
        MessageProto.InstallSnapshot.Builder builder = MessageProto.InstallSnapshot.newBuilder();
        builder.setTerm(message.getTerm());
        builder.setLeaderId(message.getLeaderId());
        builder.setLastIncludedIndex(message.getLastIncludedIndex());
        builder.setLastIncludedTerm(message.getLastIncludedTerm());
        builder.setOffset(message.getOffset());
        if (message.getData() != null) {
            builder.setData(ByteString.copyFrom(message.getData()));
        }
        builder.setDone(message.isDone());

        return builder.build().toByteString();
    }

    private RaftMessage.InstallSnapshot decodeInstallSnapshot(ByteString data) throws IOException {
        MessageProto.InstallSnapshot protoMessage = MessageProto.InstallSnapshot.parseFrom(data);
        return new RaftMessage.InstallSnapshot(
                protoMessage.getTerm(),
                protoMessage.getLeaderId(),
                protoMessage.getLastIncludedIndex(),
                protoMessage.getLastIncludedTerm(),
                protoMessage.getOffset(),
                protoMessage.getData().toByteArray(),
                protoMessage.getDone()
        );
    }

    private ByteString encodeInstallSnapshotResponse(RaftMessage.InstallSnapshotResponse message) {
        MessageProto.InstallSnapshotResponse.Builder builder = MessageProto.InstallSnapshotResponse.newBuilder();
        builder.setTerm(message.getTerm());
        return builder.build().toByteString();
    }

    private RaftMessage.InstallSnapshotResponse decodeInstallSnapshotResponse(ByteString data) throws IOException {
        MessageProto.InstallSnapshotResponse protoMessage = MessageProto.InstallSnapshotResponse.parseFrom(data);
        return new RaftMessage.InstallSnapshotResponse(protoMessage.getTerm());
    }

    private ByteString encodeLeaderInfoRequest(RaftMessage.LeaderInfoRequest message) {
        MessageProto.LeaderInfoRequest.Builder builder = MessageProto.LeaderInfoRequest.newBuilder();
        return builder.build().toByteString();
    }

    private RaftMessage.LeaderInfoRequest decodeLeaderInfoRequest(ByteString data) throws IOException {
        MessageProto.LeaderInfoRequest.parseFrom(data);
        return new RaftMessage.LeaderInfoRequest();
    }

    private ByteString encodeLeaderInfoResponse(RaftMessage.LeaderInfoResponse message) {
        MessageProto.LeaderInfoResponse.Builder builder = MessageProto.LeaderInfoResponse.newBuilder();
        builder.setTerm(message.getTerm());
        builder.setCommitIndex(message.getCommitIndex());
        builder.setTimestamp(message.getTimestamp());
        return builder.build().toByteString();
    }

    private RaftMessage.LeaderInfoResponse decodeLeaderInfoResponse(ByteString data) throws IOException {
        MessageProto.LeaderInfoResponse protoMessage = MessageProto.LeaderInfoResponse.parseFrom(data);
        return new RaftMessage.LeaderInfoResponse(
                protoMessage.getTerm(),
                protoMessage.getCommitIndex(),
                protoMessage.getTimestamp()
        );
    }

    private ByteString encodeReadIndexRequest(RaftMessage.ReadIndexRequest message) {
        MessageProto.ReadIndexRequest.Builder builder = MessageProto.ReadIndexRequest.newBuilder();
        if (message.getKey() != null) {
            builder.setKey(ByteString.copyFrom(message.getKey()));
        }
        return builder.build().toByteString();
    }

    private RaftMessage.ReadIndexRequest decodeReadIndexRequest(ByteString data) throws IOException {
        MessageProto.ReadIndexRequest protoMessage = MessageProto.ReadIndexRequest.parseFrom(data);
        return new RaftMessage.ReadIndexRequest(protoMessage.getKey().toByteArray());
    }

    private ByteString encodeReadIndexResponse(RaftMessage.ReadIndexResponse message) {
        MessageProto.ReadIndexResponse.Builder builder = MessageProto.ReadIndexResponse.newBuilder();
        builder.setTerm(message.getTerm());

        // Encode status
        MessageProto.Status.Builder statusBuilder = MessageProto.Status.newBuilder();
        statusBuilder.setCode(message.getStatus().getCode());
        statusBuilder.setMessage(message.getStatus().getMessage());
        builder.setStatus(statusBuilder.build());

        // Encode value if present
        if (message.getValue() != null) {
            builder.setValue(ByteString.copyFrom(message.getValue()));
        }

        return builder.build().toByteString();
    }

    private RaftMessage.ReadIndexResponse decodeReadIndexResponse(ByteString data) throws IOException {
        MessageProto.ReadIndexResponse protoMessage = MessageProto.ReadIndexResponse.parseFrom(data);

        // Decode status
        Status status = new Status(
                protoMessage.getStatus().getCode(),
                protoMessage.getStatus().getMessage()
        );

        // Decode value if present
        byte[] value = null;
        if (protoMessage.hasValue()) {
            value = protoMessage.getValue().toByteArray();
        }

        return new RaftMessage.ReadIndexResponse(
                protoMessage.getTerm(),
                status,
                value
        );
    }

    // KV message encoding methods

    private ByteString encodeGetRequest(KVMessage.GetRequest message) {
        MessageProto.GetRequest.Builder builder = MessageProto.GetRequest.newBuilder();
        if (message.getKey() != null) {
            builder.setKey(ByteString.copyFrom(message.getKey()));
        }
        return builder.build().toByteString();
    }

    private KVMessage.GetRequest decodeGetRequest(ByteString data) throws IOException {
        MessageProto.GetRequest protoMessage = MessageProto.GetRequest.parseFrom(data);
        return new KVMessage.GetRequest(protoMessage.getKey().toByteArray());
    }

    private ByteString encodeGetResponse(KVMessage.GetResponse message) {
        MessageProto.GetResponse.Builder builder = MessageProto.GetResponse.newBuilder();

        // Encode status
        MessageProto.Status.Builder statusBuilder = MessageProto.Status.newBuilder();
        statusBuilder.setCode(message.getStatus().getCode());
        statusBuilder.setMessage(message.getStatus().getMessage());
        builder.setStatus(statusBuilder.build());

        // Encode value if present
        if (message.getValue() != null) {
            builder.setValue(ByteString.copyFrom(message.getValue()));
        }

        return builder.build().toByteString();
    }

    private KVMessage.GetResponse decodeGetResponse(ByteString data) throws IOException {
        MessageProto.GetResponse protoMessage = MessageProto.GetResponse.parseFrom(data);

        // Decode status
        Status status = new Status(
                protoMessage.getStatus().getCode(),
                protoMessage.getStatus().getMessage()
        );

        // Decode value if present
        byte[] value = null;
        if (protoMessage.hasValue()) {
            value = protoMessage.getValue().toByteArray();
        }

        return new KVMessage.GetResponse(status, value);
    }

    private ByteString encodePutRequest(KVMessage.PutRequest message) {
        MessageProto.PutRequest.Builder builder = MessageProto.PutRequest.newBuilder();
        if (message.getKey() != null) {
            builder.setKey(ByteString.copyFrom(message.getKey()));
        }
        if (message.getValue() != null) {
            builder.setValue(ByteString.copyFrom(message.getValue()));
        }
        return builder.build().toByteString();
    }

    private KVMessage.PutRequest decodePutRequest(ByteString data) throws IOException {
        MessageProto.PutRequest protoMessage = MessageProto.PutRequest.parseFrom(data);
        return new KVMessage.PutRequest(
                protoMessage.getKey().toByteArray(),
                protoMessage.getValue().toByteArray()
        );
    }

    private ByteString encodePutResponse(KVMessage.PutResponse message) {
        MessageProto.PutResponse.Builder builder = MessageProto.PutResponse.newBuilder();

        // Encode status
        MessageProto.Status.Builder statusBuilder = MessageProto.Status.newBuilder();
        statusBuilder.setCode(message.getStatus().getCode());
        statusBuilder.setMessage(message.getStatus().getMessage());
        builder.setStatus(statusBuilder.build());

        return builder.build().toByteString();
    }

    private KVMessage.PutResponse decodePutResponse(ByteString data) throws IOException {
        MessageProto.PutResponse protoMessage = MessageProto.PutResponse.parseFrom(data);

        // Decode status
        Status status = new Status(
                protoMessage.getStatus().getCode(),
                protoMessage.getStatus().getMessage()
        );

        return new KVMessage.PutResponse(status);
    }

    private ByteString encodeDeleteRequest(KVMessage.DeleteRequest message) {
        MessageProto.DeleteRequest.Builder builder = MessageProto.DeleteRequest.newBuilder();
        if (message.getKey() != null) {
            builder.setKey(ByteString.copyFrom(message.getKey()));
        }
        return builder.build().toByteString();
    }

    private KVMessage.DeleteRequest decodeDeleteRequest(ByteString data) throws IOException {
        MessageProto.DeleteRequest protoMessage = MessageProto.DeleteRequest.parseFrom(data);
        return new KVMessage.DeleteRequest(protoMessage.getKey().toByteArray());
    }

    private ByteString encodeDeleteResponse(KVMessage.DeleteResponse message) {
        MessageProto.DeleteResponse.Builder builder = MessageProto.DeleteResponse.newBuilder();

        // Encode status
        MessageProto.Status.Builder statusBuilder = MessageProto.Status.newBuilder();
        statusBuilder.setCode(message.getStatus().getCode());
        statusBuilder.setMessage(message.getStatus().getMessage());
        builder.setStatus(statusBuilder.build());

        return builder.build().toByteString();
    }

    private KVMessage.DeleteResponse decodeDeleteResponse(ByteString data) throws IOException {
        MessageProto.DeleteResponse protoMessage = MessageProto.DeleteResponse.parseFrom(data);

        // Decode status
        Status status = new Status(
                protoMessage.getStatus().getCode(),
                protoMessage.getStatus().getMessage()
        );

        return new KVMessage.DeleteResponse(status);
    }

    private ByteString encodeScanRequest(KVMessage.ScanRequest message) {
        MessageProto.ScanRequest.Builder builder = MessageProto.ScanRequest.newBuilder();
        if (message.getStartKey() != null) {
            builder.setStartKey(ByteString.copyFrom(message.getStartKey()));
        }
        if (message.getEndKey() != null) {
            builder.setEndKey(ByteString.copyFrom(message.getEndKey()));
        }
        builder.setLimit(message.getLimit());
        return builder.build().toByteString();
    }

    private KVMessage.ScanRequest decodeScanRequest(ByteString data) throws IOException {
        MessageProto.ScanRequest protoMessage = MessageProto.ScanRequest.parseFrom(data);
        byte[] startKey = protoMessage.hasStartKey() ? protoMessage.getStartKey().toByteArray() : null;
        byte[] endKey = protoMessage.hasEndKey() ? protoMessage.getEndKey().toByteArray() : null;
        return new KVMessage.ScanRequest(startKey, endKey, protoMessage.getLimit());
    }

    private ByteString encodeScanResponse(KVMessage.ScanResponse message) {
        MessageProto.ScanResponse.Builder builder = MessageProto.ScanResponse.newBuilder();

        // Encode status
        MessageProto.Status.Builder statusBuilder = MessageProto.Status.newBuilder();
        statusBuilder.setCode(message.getStatus().getCode());
        statusBuilder.setMessage(message.getStatus().getMessage());
        builder.setStatus(statusBuilder.build());

        // Encode key-value pairs
        if (message.getEntries() != null) {
            for (Map.Entry<byte[], byte[]> entry : message.getEntries()) {
                MessageProto.KeyValue.Builder kvBuilder = MessageProto.KeyValue.newBuilder();
                if (entry.getKey() != null) {
                    kvBuilder.setKey(ByteString.copyFrom(entry.getKey()));
                }
                if (entry.getValue() != null) {
                    kvBuilder.setValue(ByteString.copyFrom(entry.getValue()));
                }
                builder.addEntries(kvBuilder.build());
            }
        }

        return builder.build().toByteString();
    }

    private KVMessage.ScanResponse decodeScanResponse(ByteString data) throws IOException {
        MessageProto.ScanResponse protoMessage = MessageProto.ScanResponse.parseFrom(data);

        // Decode status
        Status status = new Status(
                protoMessage.getStatus().getCode(),
                protoMessage.getStatus().getMessage()
        );

        // Decode key-value pairs
        List<Map.Entry<byte[], byte[]>> entries = new ArrayList<>();
        for (int i = 0; i < protoMessage.getEntriesCount(); i++) {
            MessageProto.KeyValue kv = protoMessage.getEntries(i);
            entries.add(Map.entry(
                    kv.getKey().toByteArray(),
                    kv.getValue().toByteArray()
            ));
        }

        return new KVMessage.ScanResponse(status, entries);
    }

    // Helper method to get message type ID from class
    private int getMessageType(Class<?> messageClass) {
        for (Map.Entry<Integer, Class<?>> entry : TYPE_REGISTRY.entrySet()) {
            if (entry.getValue().equals(messageClass)) {
                return entry.getKey();
            }
        }
        throw new IllegalArgumentException("Unknown message class: " + messageClass.getName());
    }
}