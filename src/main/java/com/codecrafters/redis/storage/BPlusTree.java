package com.codecrafters.redis.storage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Ordered in-memory B+ tree used by both the LSM memtable and live key index. */
final class BPlusTree<V> {
    private static final int MAX_KEYS = 31;
    private static final Object REMOVED = new Object();

    private abstract static class Node {
        final List<String> keys = new ArrayList<>();
        boolean leaf() { return this instanceof Leaf; }
    }

    private static final class Leaf extends Node {
        final List<Object> values = new ArrayList<>();
        Leaf next;
    }

    private static final class Branch extends Node {
        final List<Node> children = new ArrayList<>();
    }

    private Node root = new Leaf();
    private Leaf firstLeaf = (Leaf) root;
    private int size;
    private int tombstones;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    @SuppressWarnings("unchecked")
    V get(String key) {
        lock.readLock().lock();
        try {
            Leaf leaf = findLeaf(key);
            int index = Collections.binarySearch(leaf.keys, key);
            if (index < 0 || leaf.values.get(index) == REMOVED) return null;
            return (V) leaf.values.get(index);
        } finally { lock.readLock().unlock(); }
    }

    boolean containsKey(String key) {
        lock.readLock().lock();
        try {
            Leaf leaf = findLeaf(key);
            int index = Collections.binarySearch(leaf.keys, key);
            return index >= 0 && leaf.values.get(index) != REMOVED;
        } finally { lock.readLock().unlock(); }
    }

    void put(String key, V value) {
        lock.writeLock().lock();
        try {
            Split split = insert(root, key, value);
            if (split != null) {
                Branch newRoot = new Branch();
                newRoot.keys.add(split.separator);
                newRoot.children.add(root);
                newRoot.children.add(split.right);
                root = newRoot;
            }
        } finally { lock.writeLock().unlock(); }
    }

    @SuppressWarnings("unchecked")
    V remove(String key) {
        lock.writeLock().lock();
        try { return removeLocked(key); }
        finally { lock.writeLock().unlock(); }
    }

    V removeIfSame(String key, V expected) {
        lock.writeLock().lock();
        try {
            Leaf leaf = findLeaf(key);
            int index = Collections.binarySearch(leaf.keys, key);
            if (index < 0 || leaf.values.get(index) != expected) return null;
            return removeLocked(key);
        } finally { lock.writeLock().unlock(); }
    }

    @SuppressWarnings("unchecked")
    private V removeLocked(String key) {
        Leaf leaf = findLeaf(key);
        int index = Collections.binarySearch(leaf.keys, key);
        if (index < 0 || leaf.values.get(index) == REMOVED) return null;
        Object previous = leaf.values.set(index, REMOVED);
        size--;
        tombstones++;
        if (tombstones > 1024 && tombstones > size / 3) compactLocked();
        return (V) previous;
    }

    /** Rebuilds the tree when removed unique keys would otherwise accumulate forever. */
    @SuppressWarnings("unchecked")
    private void compactLocked() {
        List<Map.Entry<String, V>> live = new ArrayList<>(size);
        for (Leaf leaf = firstLeaf; leaf != null; leaf = leaf.next) {
            for (int i = 0; i < leaf.keys.size(); i++) {
                Object value = leaf.values.get(i);
                if (value != REMOVED) live.add(Map.entry(leaf.keys.get(i), (V) value));
            }
        }
        root = new Leaf();
        firstLeaf = (Leaf) root;
        size = 0;
        tombstones = 0;
        for (Map.Entry<String, V> entry : live) {
            Split split = insert(root, entry.getKey(), entry.getValue());
            if (split != null) {
                Branch newRoot = new Branch();
                newRoot.keys.add(split.separator);
                newRoot.children.add(root);
                newRoot.children.add(split.right);
                root = newRoot;
            }
        }
    }

    int size() { lock.readLock().lock(); try { return size; } finally { lock.readLock().unlock(); } }

    @SuppressWarnings("unchecked")
    NavigableMap<String, V> snapshot() {
        lock.readLock().lock();
        try {
            NavigableMap<String, V> result = new TreeMap<>();
            for (Leaf leaf = firstLeaf; leaf != null; leaf = leaf.next) {
                for (int i = 0; i < leaf.keys.size(); i++) {
                    Object value = leaf.values.get(i);
                    if (value != REMOVED) result.put(leaf.keys.get(i), (V) value);
                }
            }
            return Collections.unmodifiableNavigableMap(result);
        } finally { lock.readLock().unlock(); }
    }

    void clear() {
        lock.writeLock().lock();
        try {
            root = new Leaf();
            firstLeaf = (Leaf) root;
            size = 0;
            tombstones = 0;
        } finally { lock.writeLock().unlock(); }
    }

    private Leaf findLeaf(String key) {
        Node node = root;
        while (node instanceof Branch branch) node = branch.children.get(childIndex(branch.keys, key));
        return (Leaf) node;
    }

    private Split insert(Node node, String key, V value) {
        if (node instanceof Leaf leaf) {
            int index = Collections.binarySearch(leaf.keys, key);
            if (index >= 0) {
                if (leaf.values.get(index) == REMOVED) { size++; tombstones--; }
                leaf.values.set(index, value);
                return null;
            }
            index = -index - 1;
            leaf.keys.add(index, key);
            leaf.values.add(index, value);
            size++;
            if (leaf.keys.size() <= MAX_KEYS) return null;

            Leaf right = new Leaf();
            int mid = leaf.keys.size() / 2;
            right.keys.addAll(leaf.keys.subList(mid, leaf.keys.size()));
            right.values.addAll(leaf.values.subList(mid, leaf.values.size()));
            leaf.keys.subList(mid, leaf.keys.size()).clear();
            leaf.values.subList(mid, leaf.values.size()).clear();
            right.next = leaf.next;
            leaf.next = right;
            return new Split(right.keys.get(0), right);
        }

        Branch branch = (Branch) node;
        int child = childIndex(branch.keys, key);
        Split childSplit = insert(branch.children.get(child), key, value);
        if (childSplit == null) return null;
        branch.keys.add(child, childSplit.separator);
        branch.children.add(child + 1, childSplit.right);
        if (branch.keys.size() <= MAX_KEYS) return null;

        int mid = branch.keys.size() / 2;
        String separator = branch.keys.get(mid);
        Branch right = new Branch();
        right.keys.addAll(branch.keys.subList(mid + 1, branch.keys.size()));
        right.children.addAll(branch.children.subList(mid + 1, branch.children.size()));
        branch.keys.subList(mid, branch.keys.size()).clear();
        branch.children.subList(mid + 1, branch.children.size()).clear();
        return new Split(separator, right);
    }

    private static int childIndex(List<String> separators, String key) {
        int low = 0, high = separators.size();
        while (low < high) {
            int mid = (low + high) >>> 1;
            if (key.compareTo(separators.get(mid)) >= 0) low = mid + 1;
            else high = mid;
        }
        return low;
    }

    private record Split(String separator, Node right) {}
}
