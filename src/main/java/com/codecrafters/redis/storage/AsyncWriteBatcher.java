package com.codecrafters.redis.storage;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Bounded asynchronous write queue. Futures complete only after the batch WAL force. */
public final class AsyncWriteBatcher implements AutoCloseable {
    private final KeyValueAccess store;
    private final ArrayBlockingQueue<Request> queue;
    private final int maxBatchSize;
    private final long maxDelayMillis;
    private final AtomicBoolean accepting = new AtomicBoolean(true);
    private final Thread worker;

    public AsyncWriteBatcher(KeyValueAccess store, int queueCapacity, int maxBatchSize, long maxDelayMillis) {
        if (store == null || queueCapacity < 1 || maxBatchSize < 1 || maxDelayMillis < 0)
            throw new IllegalArgumentException("Invalid write batcher configuration");
        this.store = store;
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        this.maxBatchSize = maxBatchSize;
        this.maxDelayMillis = maxDelayMillis;
        this.worker = new Thread(this::run, "redis-write-batcher");
        this.worker.setDaemon(true);
        this.worker.start();
    }

    public CompletableFuture<Void> submit(String key, String value) {
        if (key == null || value == null) throw new IllegalArgumentException("Key and value cannot be null");
        if (!accepting.get()) throw new IllegalStateException("Write batcher is closed");
        Request request = new Request(key, value);
        try {
            if (!queue.offer(request, 5, TimeUnit.SECONDS))
                throw new IllegalStateException("Write queue is full");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while queuing write", e);
        }
        if (!accepting.get() && queue.remove(request))
            throw new IllegalStateException("Write batcher is closed");
        return request.result;
    }

    private void run() {
        while (accepting.get() || !queue.isEmpty()) {
            try {
                Request first = queue.poll(100, TimeUnit.MILLISECONDS);
                if (first == null) continue;
                List<Request> batch = new ArrayList<>(maxBatchSize);
                batch.add(first);
                long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(maxDelayMillis);
                while (batch.size() < maxBatchSize) {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) break;
                    Request next = queue.poll(remaining, TimeUnit.NANOSECONDS);
                    if (next == null) break;
                    batch.add(next);
                }
                Map<String, String> writes = new LinkedHashMap<>();
                for (Request request : batch) writes.put(request.key, request.value);
                try {
                    store.setBatch(writes);
                    batch.forEach(request -> request.result.complete(null));
                } catch (Throwable failure) {
                    batch.forEach(request -> request.result.completeExceptionally(failure));
                }
            } catch (InterruptedException e) {
                // close() interrupts only to wake the worker; queued writes are drained.
            }
        }
    }

    @Override public synchronized void close() {
        if (!accepting.getAndSet(false)) return;
        boolean interrupted = false;
        while (worker.isAlive()) {
            try {
                worker.join();
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) Thread.currentThread().interrupt();
    }

    private static final class Request {
        private final String key, value;
        private final CompletableFuture<Void> result = new CompletableFuture<>();
        private Request(String key, String value) { this.key = key; this.value = value; }
    }
}
