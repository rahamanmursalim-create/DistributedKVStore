package com.distributedkv.common;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * A thread-safe implementation of a hash set using ConcurrentHashMap
 *
 * @param <E> the type of elements maintained by this set
 */
public class ConcurrentHashSet<E> implements Set<E>, Serializable {
    private static final long serialVersionUID = 1L;

    // Backing ConcurrentHashMap to store the elements
    private final Map<E, Boolean> map;

    /**
     * Constructs an empty set with default initial capacity and load factor
     */
    public ConcurrentHashSet() {
        map = new ConcurrentHashMap<>();
    }

    /**
     * Constructs an empty set with the specified initial capacity and default load factor
     *
     * @param initialCapacity the initial capacity
     */
    public ConcurrentHashSet(int initialCapacity) {
        map = new ConcurrentHashMap<>(initialCapacity);
    }

    /**
     * Constructs an empty set with the specified initial capacity and load factor
     *
     * @param initialCapacity the initial capacity
     * @param loadFactor the load factor
     */
    public ConcurrentHashSet(int initialCapacity, float loadFactor) {
        map = new ConcurrentHashMap<>(initialCapacity, loadFactor);
    }

    /**
     * Constructs a new set containing the elements in the specified collection
     *
     * @param c the collection whose elements are to be placed into this set
     */
    public ConcurrentHashSet(Collection<? extends E> c) {
        map = new ConcurrentHashMap<>(Math.max((int) (c.size() / 0.75f) + 1, 16));
        addAll(c);
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return map.containsKey(o);
    }

    @Override
    public Iterator<E> iterator() {
        return map.keySet().iterator();
    }

    @Override
    public Object[] toArray() {
        return map.keySet().toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return map.keySet().toArray(a);
    }

    @Override
    public boolean add(E e) {
        return map.put(e, Boolean.TRUE) == null;
    }

    @Override
    public boolean remove(Object o) {
        return map.remove(o) != null;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return map.keySet().containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        boolean modified = false;
        for (E e : c) {
            if (add(e)) {
                modified = true;
            }
        }
        return modified;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return map.keySet().retainAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return map.keySet().removeAll(c);
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public Spliterator<E> spliterator() {
        return map.keySet().spliterator();
    }

    @Override
    public boolean removeIf(Predicate<? super E> filter) {
        return map.keySet().removeIf(filter);
    }

    @Override
    public Stream<E> stream() {
        return map.keySet().stream();
    }

    @Override
    public Stream<E> parallelStream() {
        return map.keySet().parallelStream();
    }

    @Override
    public void forEach(Consumer<? super E> action) {
        map.keySet().forEach(action);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof Set)) {
            return false;
        }

        Set<?> other = (Set<?>) o;
        if (other.size() != size()) {
            return false;
        }

        try {
            return containsAll(other);
        } catch (ClassCastException | NullPointerException unused) {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return map.keySet().hashCode();
    }

    @Override
    public String toString() {
        return map.keySet().toString();
    }

    /**
     * Returns a shallow copy of this HashSet instance
     *
     * @return a shallow copy of this set
     */
    public Object clone() {
        return new ConcurrentHashSet<>(this);
    }

    /**
     * Attempts to compute a mapping for the specified key and its current
     * mapped value (or null if no current mapping exists).
     *
     * @param e element to be inserted if it doesn't exist
     * @return true if element was added, false if it already exists
     */
    public boolean putIfAbsent(E e) {
        return map.putIfAbsent(e, Boolean.TRUE) == null;
    }
}