package com.distributedkv.raft.election;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implements the election timer for the Raft consensus algorithm.
 *
 * The election timer is responsible for triggering elections when a follower
 * hasn't heard from the leader for a certain period of time. The timer is
 * reset whenever a valid AppendEntries RPC is received from the current leader.
 */
public class ElectionTimer {
    private static final Logger LOGGER = Logger.getLogger(ElectionTimer.class.getName());

    /** The executor for scheduling timer tasks */
    private final ScheduledExecutorService executor;

    /** The minimum election timeout in milliseconds */
    private final int minElectionTimeoutMs;

    /** The maximum election timeout in milliseconds */
    private final int maxElectionTimeoutMs;

    /** The action to execute when the timer expires */
    private final Supplier<CompletableFuture<Boolean>> electionStartAction;

    /** Random number generator for timeout randomization */
    private final Random random = new Random();

    /** The current scheduled timer task */
    private volatile ScheduledFuture<?> scheduledTask;

    /** Flag indicating whether the timer is active */
    private final AtomicBoolean active = new AtomicBoolean(false);

    /** Flag indicating whether an election is in progress */
    private final AtomicBoolean electionInProgress = new AtomicBoolean(false);

    /**
     * Creates a new election timer.
     *
     * @param executor The executor
     * @param minElectionTimeoutMs The minimum election timeout
     * @param maxElectionTimeoutMs The maximum election timeout
     * @param electionStartAction The action to execute when the timer expires
     */
    public ElectionTimer(ScheduledExecutorService executor, int minElectionTimeoutMs,
                         int maxElectionTimeoutMs, Supplier<CompletableFuture<Boolean>> electionStartAction) {
        this.executor = executor;
        this.minElectionTimeoutMs = minElectionTimeoutMs;
        this.maxElectionTimeoutMs = maxElectionTimeoutMs;
        this.electionStartAction = electionStartAction;
    }

    /**
     * Starts the election timer.
     */
    public void start() {
        if (active.compareAndSet(false, true)) {
            LOGGER.info("Starting election timer");
            resetTimeout();
        }
    }

    /**
     * Stops the election timer.
     */
    public void stop() {
        if (active.compareAndSet(true, false)) {
            LOGGER.info("Stopping election timer");
            cancelTimeout();
        }
    }

    /**
     * Resets the election timeout.
     */
    public void resetTimeout() {
        if (!active.get()) {
            return;
        }

        cancelTimeout();

        // Calculate a random timeout within the specified range
        int timeoutMs = minElectionTimeoutMs + random.nextInt(maxElectionTimeoutMs - minElectionTimeoutMs + 1);

        // Schedule a new timeout
        scheduledTask = executor.schedule(this::timerElapsed, timeoutMs, TimeUnit.MILLISECONDS);

        LOGGER.fine("Reset election timeout to " + timeoutMs + "ms");
    }

    /**
     * Cancels the current timeout.
     */
    private void cancelTimeout() {
        ScheduledFuture<?> task = scheduledTask;
        if (task != null) {
            task.cancel(false);
            scheduledTask = null;
        }
    }

    /**
     * Called when the election timeout elapses.
     */
    private void timerElapsed() {
        if (!active.get() || electionInProgress.get()) {
            return;
        }

        LOGGER.info("Election timeout elapsed, starting election");

        if (electionInProgress.compareAndSet(false, true)) {
            try {
                // Start the election
                electionStartAction.get()
                        .whenComplete((elected, ex) -> {
                            electionInProgress.set(false);

                            if (ex != null) {
                                LOGGER.log(Level.WARNING, "Error during election", ex);
                            } else if (elected) {
                                LOGGER.info("Election succeeded, became leader");
                            } else {
                                LOGGER.info("Election failed, resetting timeout");
                                resetTimeout();
                            }
                        });
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Failed to start election", e);
                electionInProgress.set(false);
                resetTimeout();
            }
        }
    }

    /**
     * Checks if an election is currently in progress.
     *
     * @return True if an election is in progress
     */
    public boolean isElectionInProgress() {
        return electionInProgress.get();
    }

    /**
     * Checks if the timer is active.
     *
     * @return True if the timer is active
     */
    public boolean isActive() {
        return active.get();
    }
}