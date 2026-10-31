package com.distributedkv.common;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Implementation of consistent hashing for distributing keys across nodes
 *
 * @param <T> the type of nodes
 */
public class ConsistentHash<T> {
    // Number of virtual nodes per physical node
    private final int numberOfVirtualNodes;

    // Hash ring implementation using a sorted map
    private final SortedMap<Long, T> hashRing;

    // Map of physical nodes to their virtual nodes
    private final Map<T, List<Long>> nodeToVirtualNodes;

    // Lock for thread safety
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    // MD5 hash function
    private final MessageDigest md5;

    /**
     * Constructor with custom virtual node count
     *
     * @param numberOfVirtualNodes number of virtual nodes per physical node
     */
    public ConsistentHash(int numberOfVirtualNodes) {
        this.numberOfVirtualNodes = numberOfVirtualNodes;
        this.hashRing = new ConcurrentSkipListMap<>();
        this.nodeToVirtualNodes = new ConcurrentHashMap<>();

        try {
            this.md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 algorithm not available", e);
        }
    }

    /**
     * Default constructor with 100 virtual nodes
     */
    public ConsistentHash() {
        this(100);
    }

    /**
     * Add a node to the hash ring
     *
     * @param node the node to add
     */
    public void addNode(T node) {
        if (node == null) {
            throw new IllegalArgumentException("Node cannot be null");
        }

        lock.writeLock().lock();
        try {
            // Create virtual nodes
            List<Long> virtualNodes = new ArrayList<>(numberOfVirtualNodes);
            for (int i = 0; i < numberOfVirtualNodes; i++) {
                long hash = hashFunction(getVirtualNodeName(node, i));
                hashRing.put(hash, node);
                virtualNodes.add(hash);
            }
            nodeToVirtualNodes.put(node, virtualNodes);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Add multiple nodes to the hash ring
     *
     * @param nodes the nodes to add
     */
    public void addNodes(Collection<T> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            return;
        }

        for (T node : nodes) {
            addNode(node);
        }
    }

    /**
     * Remove a node from the hash ring
     *
     * @param node the node to remove
     */
    public void removeNode(T node) {
        if (node == null) {
            throw new IllegalArgumentException("Node cannot be null");
        }

        lock.writeLock().lock();
        try {
            List<Long> virtualNodes = nodeToVirtualNodes.remove(node);
            if (virtualNodes != null) {
                for (Long hash : virtualNodes) {
                    hashRing.remove(hash);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Get the node responsible for a key
     *
     * @param key the key to look up
     * @return the node responsible for the key, or null if no nodes exist
     */
    public T getNode(String key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        lock.readLock().lock();
        try {
            if (hashRing.isEmpty()) {
                return null;
            }

            long hash = hashFunction(key);

            // Find the first node with hash >= key's hash
            SortedMap<Long, T> tailMap = hashRing.tailMap(hash);
            Long nodeHash = tailMap.isEmpty() ? hashRing.firstKey() : tailMap.firstKey();

            return hashRing.get(nodeHash);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get the node responsible for a key using byte array representation
     *
     * @param key the key to look up as byte array
     * @return the node responsible for the key, or null if no nodes exist
     */
    public T getNode(byte[] key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        return getNode(new String(key, StandardCharsets.UTF_8));
    }

    /**
     * Get multiple nodes for replication of a key
     *
     * @param key the key to look up
     * @param count the number of nodes to return
     * @return a list of nodes, which may be smaller than count if there aren't enough nodes
     */
    public List<T> getNodes(String key, int count) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        if (count <= 0) {
            throw new IllegalArgumentException("Count must be positive");
        }

        lock.readLock().lock();
        try {
            // Create a list to track unique nodes
            List<T> result = new ArrayList<>(count);
            if (hashRing.isEmpty()) {
                return result;
            }

            // Number of unique physical nodes available
            int availableNodes = nodeToVirtualNodes.size();
            count = Math.min(count, availableNodes);

            // No need to continue if we need 0 nodes
            if (count == 0) {
                return result;
            }

            long hash = hashFunction(key);

            // Start at the hash and go clockwise to find nodes
            SortedMap<Long, T> tailMap = new TreeMap<>(hashRing.tailMap(hash));
            tailMap.putAll(hashRing.headMap(hash));

            // Add nodes to the result, skipping duplicates
            for (Map.Entry<Long, T> entry : tailMap.entrySet()) {
                if (!result.contains(entry.getValue())) {
                    result.add(entry.getValue());
                    if (result.size() == count) {
                        break;
                    }
                }
            }

            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get multiple nodes for replication of a key as byte array
     *
     * @param key the key to look up as byte array
     * @param count the number of nodes to return
     * @return a list of nodes, which may be smaller than count if there aren't enough nodes
     */
    public List<T> getNodes(byte[] key, int count) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        return getNodes(new String(key, StandardCharsets.UTF_8), count);
    }

    /**
     * Get all physical nodes in the hash ring
     *
     * @return a list of all physical nodes
     */
    public List<T> getAllNodes() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(nodeToVirtualNodes.keySet());
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get the number of nodes in the hash ring
     *
     * @return the number of physical nodes
     */
    public int size() {
        lock.readLock().lock();
        try {
            return nodeToVirtualNodes.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Check if the hash ring is empty
     *
     * @return true if no nodes exist, false otherwise
     */
    public boolean isEmpty() {
        lock.readLock().lock();
        try {
            return nodeToVirtualNodes.isEmpty();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get the distribution of virtual nodes across physical nodes
     *
     * @return a map of physical nodes to their virtual node counts
     */
    public Map<T, Integer> getNodeDistribution() {
        lock.readLock().lock();
        try {
            Map<T, Integer> distribution = new HashMap<>();
            for (Map.Entry<T, List<Long>> entry : nodeToVirtualNodes.entrySet()) {
                distribution.put(entry.getKey(), entry.getValue().size());
            }
            return distribution;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Generate a virtual node name
     *
     * @param node the physical node
     * @param index the virtual node index
     * @return the virtual node name
     */
    private String getVirtualNodeName(T node, int index) {
        return node.toString() + "#" + index;
    }

    /**
     * Compute the hash of a key
     *
     * @param key the key to hash
     * @return the hash value
     */
    private long hashFunction(String key) {
        byte[] digest;
        lock.readLock().lock();
        try {
            md5.reset();
            digest = md5.digest(key.getBytes(StandardCharsets.UTF_8));
        } finally {
            lock.readLock().unlock();
        }

        // Use the first 8 bytes of the MD5 hash as a long
        long hash = 0;
        for (int i = 0; i < 8; i++) {
            hash <<= 8;
            hash |= (digest[i] & 0xFF);
        }
        return hash;
    }

    /**
     * Check if a node exists in the hash ring
     *
     * @param node the node to check
     * @return true if the node exists
     */
    public boolean containsNode(T node) {
        if (node == null) {
            return false;
        }

        lock.readLock().lock();
        try {
            return nodeToVirtualNodes.containsKey(node);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Clear all nodes from the hash ring
     */
    public void clear() {
        lock.writeLock().lock();
        try {
            hashRing.clear();
            nodeToVirtualNodes.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }
}