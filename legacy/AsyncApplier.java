package com.distributedkv.raft;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distributedkv.common.Metrics;
import com.distributedkv.common.MetricsRegistry;
import com.distributedkv.network.protocol.AsyncApplyNotificationMessage;

/**
 * Manages asynchronous application of log entries to the state machine.
 *
 * This class implements the Async Apply optimization, which allows log entries
 * to be applied to the state machine without blocking the Raft consensus protocol.
 * This can significantly improve throughput by allowing the protocol to continue
 * operating while entries are being applied.
 */
public class AsyncApplier {
    private static final Logger LOGGER = Logger.getLogger(AsyncApplier.class.getName());

    /** The state machine to which log entries are applied */
    private final StateMachine stateMachine;

    /** The log from which entries are read */
    private final Log log;

    /** Queue of entries waiting to be applied */
    private final Queue<LogEntry> applyQueue = new ConcurrentLinkedQueue<>();

    /** Executor for async operations */
    private final ExecutorService executor;

    /** The last index that has been applied to the state machine */
    private final AtomicLong lastAppliedIndex = new AtomicLong(0);

    /** Flag indicating whether the applier is running */
    private final AtomicBoolean running = new AtomicBoolean(false);

    /** Flag indicating whether application is currently in progress */
    private final AtomicBoolean applying = new AtomicBoolean(false);

    /** Maximum number of entries to apply in a single batch */
    private final int maxBatchSize;

    /** Callback for apply completion notifications */
    private Consumer<AsyncApplyNotificationMessage> notificationCallback;

    /** Metrics for monitoring apply performance */
    private final Metrics metrics;

    /**
     * Creates a new async applier.
     *
     * @param stateMachine The state machine
     * @param log The log
     * @param maxBatchSize The maximum batch size
     */
    public AsyncApplier(StateMachine stateMachine, Log log, int maxBatchSize) {
        this.stateMachine = stateMachine;
        this.log = log;
        this.maxBatchSize = maxBatchSize;
        this.executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "async-applier");
            t.setDaemon(true);
            return t;
        });
        this.metrics = MetricsRegistry.getInstance().getOrCreateMetrics("AsyncApplier");
    }

    /**
     * Starts the async applier.
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            LOGGER.info("Starting async applier");

            // Initialize the last applied index from the state machine
            lastAppliedIndex.set(stateMachine.getLastAppliedIndex());

            // Schedule the apply loop
            scheduleApply();
        }
    }

    /**
     * Stops the async applier.
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            LOGGER.info("Stopping async applier");
            executor.shutdown();
        }
    }

    /**
     * Sets the notification callback.
     *
     * @param callback The callback
     */
    public void setNotificationCallback(Consumer<AsyncApplyNotificationMessage> callback) {
        this.notificationCallback = callback;
    }

    /**
     * Schedules the apply loop.
     */
    private void scheduleApply() {
        if (!running.get() || applying.get()) {
            return;
        }

        executor.submit(this::applyLoop);
    }

    /**
     * The main apply loop.
     */
    private void applyLoop() {
        if (!applying.compareAndSet(false, true)) {
            return;
        }

        try {
            long commitIndex = log.getCommitIndex();
            long lastApplied = lastAppliedIndex.get();

            // Check if there are new entries to apply
            if (commitIndex > lastApplied) {
                // Get the entries to apply
                long toIndex = Math.min(commitIndex, lastApplied + maxBatchSize);
                List<LogEntry> entries = log.getEntries(lastApplied + 1, toIndex);

                // Apply the entries
                for (LogEntry entry : entries) {
                    applyEntry(entry);
                }

                // Update the log's last applied index
                log.setLastApplied(lastAppliedIndex.get());
            }

            // Apply any entries in the queue
            int batchSize = 0;
            while (batchSize < maxBatchSize && !applyQueue.isEmpty()) {
                LogEntry entry = applyQueue.poll();
                if (entry != null) {
                    applyEntry(entry);
                    batchSize++;
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error in apply loop", e);
        } finally {
            applying.set(false);

            // If there are more entries to apply, schedule another apply
            if (log.getCommitIndex() > lastAppliedIndex.get() || !applyQueue.isEmpty()) {
                scheduleApply();
            }
        }
    }

    /**
     * Applies a single log entry to the state machine.
     *
     * @param entry The entry to apply
     */
    private void applyEntry(LogEntry entry) {
        if (entry.getIndex() <= lastAppliedIndex.get()) {
            // Already applied
            return;
        }

        long startTime = System.currentTimeMillis();
        boolean success = false;
        String errorMessage = null;

        try {
            // Apply the entry to the state machine
            stateMachine.apply(entry);

            // Update the last applied index
            lastAppliedIndex.set(entry.getIndex());

            success = true;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to apply entry " + entry.getIndex(), e);
            errorMessage = e.getMessage();
        } finally {
            long endTime = System.currentTimeMillis();
            long applyTime = endTime - startTime;

            // Update metrics
            metrics.histogram("apply.time").update(applyTime);
            if (success) {
                metrics.counter("apply.success").inc();
            } else {
                metrics.counter("apply.failure").inc();
            }

            // Send notification if callback is registered
            if (notificationCallback != null) {
                try {
                    AsyncApplyNotificationMessage notification;
                    if (success) {
                        notification = new AsyncApplyNotificationMessage(
                                entry.getTerm(), "", "", entry.getIndex(),
                                log.getCommitIndex(), applyTime);
                    } else {
                        notification = new AsyncApplyNotificationMessage(
                                entry.getTerm(), "", "", entry.getIndex(),
                                log.getCommitIndex(), applyTime, errorMessage);
                    }

                    notificationCallback.accept(notification);
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING, "Failed to send notification", e);
                }
            }
        }
    }

    /**
     * Queues an entry for asynchronous application.
     *
     * @param entry The entry to apply
     */
    public void queueEntry(LogEntry entry) {
        applyQueue.add(entry);
        scheduleApply();
    }

    /**
     * Applies an entry immediately and returns the result.
     *
     * @param entry The entry to apply
     * @return The result of applying the entry
     */
    public byte[] applyImmediate(LogEntry entry) {
        long startTime = System.currentTimeMillis();
        byte[] result = null;
        boolean success = false;
        String errorMessage = null;

        try {
            // Apply the entry to the state machine
            result = stateMachine.apply(entry);

            // Update the last applied index if it's higher
            lastAppliedIndex.updateAndGet(current -> Math.max(current, entry.getIndex()));

            // Update the log's last applied index
            log.setLastApplied(lastAppliedIndex.get());

            success = true;
            return result;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to apply entry " + entry.getIndex(), e);
            errorMessage = e.getMessage();
            throw e;
        } finally {
            long endTime = System.currentTimeMillis();
            long applyTime = endTime - startTime;

            // Update metrics
            metrics.histogram("apply.immediate.time").update(applyTime);
            if (success) {
                metrics.counter("apply.immediate.success").inc();
            } else {
                metrics.counter("apply.immediate.failure").inc();
            }

            // Send notification if callback is registered
            if (notificationCallback != null) {
                try {
                    AsyncApplyNotificationMessage notification;
                    if (success) {
                        notification = new AsyncApplyNotificationMessage(
                                entry.getTerm(), "", "", entry.getIndex(),
                                log.getCommitIndex(), applyTime);
                    } else {
                        notification = new AsyncApplyNotificationMessage(
                                entry.getTerm(), "", "", entry.getIndex(),
                                log.getCommitIndex(), applyTime, errorMessage);
                    }

                    notificationCallback.accept(notification);
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING, "Failed to send notification", e);
                }
            }
        }
    }

    /**
     * Applies an entry asynchronously.
     *
     * @param entry The entry to apply
     * @return A future that will complete with the result of applying the entry
     */
    public CompletableFuture<byte[]> applyAsync(LogEntry entry) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();

        executor.submit(() -> {
            try {
                byte[] result = applyImmediate(entry);
                future.complete(result);
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        });

        return future;
    }

    /**
     * Gets the last index that has been applied to the state machine.
     *
     * @return The last applied index
     */
    public long getLastAppliedIndex() {
        return lastAppliedIndex.get();
    }

    /**
     * Waits until the specified index has been applied to the state machine.
     *
     * @param index The index to wait for
     * @return A future that will complete when the index has been applied
     */
    public CompletableFuture<Void> waitForApplied(long index) {
        if (lastAppliedIndex.get() >= index) {
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<Void> future = new CompletableFuture<>();

        // Schedule a task to check periodically
        executor.submit(new Runnable() {
            @Override
            public void run() {
                if (lastAppliedIndex.get() >= index) {
                    future.complete(null);
                } else if (running.get()) {
                    // Schedule another check after a short delay
                    executor.submit(this);
                } else {
                    future.completeExceptionally(new IllegalStateException("AsyncApplier stopped"));
                }
            }
        });

        return future;
    }
}