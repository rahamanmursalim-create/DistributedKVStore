package com.distributedkv.common;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Enhanced ThreadPoolExecutor with metrics and monitoring
 */
public class ThreadPoolExecutor extends java.util.concurrent.ThreadPoolExecutor {
    // Thread factory for creating named threads
    private static class NamedThreadFactory implements ThreadFactory {
        private final String prefix;
        private final AtomicInteger counter = new AtomicInteger(0);
        private final boolean daemon;

        public NamedThreadFactory(String prefix, boolean daemon) {
            this.prefix = prefix;
            this.daemon = daemon;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r, prefix + "-" + counter.incrementAndGet());
            thread.setDaemon(daemon);
            return thread;
        }
    }

    // Metrics for this thread pool
    private final Metrics metrics;
    private final String metricsPrefix;

    // Track execution time for tasks
    private final ThreadLocal<Long> startTime = new ThreadLocal<>();

    /**
     * Create a thread pool with the specified parameters
     *
     * @param corePoolSize the core pool size
     * @param maximumPoolSize the maximum pool size
     * @param keepAliveTime the keep-alive time
     * @param unit the time unit for keep-alive time
     * @param workQueue the work queue
     * @param threadFactory the thread factory
     * @param handler the rejected execution handler
     * @param metricsName the name for metrics
     */
    public ThreadPoolExecutor(
            int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit,
            BlockingQueue<Runnable> workQueue,
            ThreadFactory threadFactory,
            RejectedExecutionHandler handler,
            String metricsName) {

        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);

        this.metrics = MetricsRegistry.getInstance().getOrCreateMetrics(metricsName);
        this.metricsPrefix = "threadpool";

        // Initialize metrics
        metrics.setGauge(metricsPrefix + ".core_pool_size", corePoolSize);
        metrics.setGauge(metricsPrefix + ".max_pool_size", maximumPoolSize);
    }

    /**
     * Create a thread pool with a default rejected execution handler
     *
     * @param corePoolSize the core pool size
     * @param maximumPoolSize the maximum pool size
     * @param keepAliveTime the keep-alive time
     * @param unit the time unit for keep-alive time
     * @param workQueue the work queue
     * @param threadFactory the thread factory
     * @param metricsName the name for metrics
     */
    public ThreadPoolExecutor(
            int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit,
            BlockingQueue<Runnable> workQueue,
            ThreadFactory threadFactory,
            String metricsName) {

        this(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory,
                new AbortPolicy(), metricsName);
    }

    /**
     * Create a thread pool with a default thread factory and rejected execution handler
     *
     * @param corePoolSize the core pool size
     * @param maximumPoolSize the maximum pool size
     * @param keepAliveTime the keep-alive time
     * @param unit the time unit for keep-alive time
     * @param workQueue the work queue
     * @param metricsName the name for metrics
     */
    public ThreadPoolExecutor(
            int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit,
            BlockingQueue<Runnable> workQueue,
            String metricsName) {

        this(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue,
                new NamedThreadFactory(metricsName, false), metricsName);
    }

    /**
     * Create a thread pool with default parameters
     *
     * @param corePoolSize the core pool size
     * @param maximumPoolSize the maximum pool size
     * @param metricsName the name for metrics
     */
    public ThreadPoolExecutor(int corePoolSize, int maximumPoolSize, String metricsName) {
        this(corePoolSize, maximumPoolSize, 60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(), metricsName);
    }

    /**
     * Create a fixed-size thread pool
     *
     * @param nThreads the number of threads
     * @param metricsName the name for metrics
     * @return a fixed-size thread pool
     */
    public static ThreadPoolExecutor newFixedThreadPool(int nThreads, String metricsName) {
        return new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), metricsName);
    }

    /**
     * Create a cached thread pool
     *
     * @param metricsName the name for metrics
     * @return a cached thread pool
     */
    public static ThreadPoolExecutor newCachedThreadPool(String metricsName) {
        return new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
                new SynchronousQueue<>(), metricsName);
    }

    /**
     * Create a single-threaded executor
     *
     * @param metricsName the name for metrics
     * @return a single-threaded executor
     */
    public static ThreadPoolExecutor newSingleThreadExecutor(String metricsName) {
        return new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), metricsName);
    }

    /**
     * Create a scheduled thread pool
     *
     * @param corePoolSize the core pool size
     * @param metricsName the name for metrics
     * @return a scheduled thread pool
     */
    public static ScheduledThreadPoolExecutor newScheduledThreadPool(int corePoolSize, String metricsName) {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(corePoolSize,
                new NamedThreadFactory(metricsName, false));

        // Register metrics
        Metrics metrics = MetricsRegistry.getInstance().getOrCreateMetrics(metricsName);
        String prefix = "threadpool";
        metrics.setGauge(prefix + ".core_pool_size", corePoolSize);

        // Register dynamic metrics supplier
        MetricsRegistry.getInstance().registerMetricsSupplier(metricsName, () -> {
            java.util.Map<String, Object> result = new java.util.HashMap<>();
            result.put(prefix + ".active_count", executor.getActiveCount());
            result.put(prefix + ".pool_size", executor.getPoolSize());
            result.put(prefix + ".largest_pool_size", executor.getLargestPoolSize());
            result.put(prefix + ".task_count", executor.getTaskCount());
            result.put(prefix + ".completed_task_count", executor.getCompletedTaskCount());
            result.put(prefix + ".queue_size", executor.getQueue().size());
            return result;
        });

        return executor;
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        super.beforeExecute(t, r);
        startTime.set(System.currentTimeMillis());
        metrics.incrementCounter(metricsPrefix + ".task_started");
        metrics.setGauge(metricsPrefix + ".active_count", getActiveCount());
        metrics.setGauge(metricsPrefix + ".pool_size", getPoolSize());
        metrics.setGauge(metricsPrefix + ".largest_pool_size", getLargestPoolSize());
        metrics.setGauge(metricsPrefix + ".task_count", getTaskCount());
        metrics.setGauge(metricsPrefix + ".completed_task_count", getCompletedTaskCount());
        metrics.setGauge(metricsPrefix + ".queue_size", getQueue().size());
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        try {
            long endTime = System.currentTimeMillis();
            Long startTimeValue = startTime.get();
            if (startTimeValue != null) {
                long elapsed = endTime - startTimeValue;
                metrics.recordTimer(metricsPrefix + ".task_execution_time", elapsed);
                startTime.remove();
            }

            metrics.incrementCounter(metricsPrefix + ".task_completed");
            if (t != null) {
                metrics.incrementCounter(metricsPrefix + ".task_failed");
            }

            metrics.setGauge(metricsPrefix + ".active_count", getActiveCount());
            metrics.setGauge(metricsPrefix + ".pool_size", getPoolSize());
            metrics.setGauge(metricsPrefix + ".task_count", getTaskCount());
            metrics.setGauge(metricsPrefix + ".completed_task_count", getCompletedTaskCount());
            metrics.setGauge(metricsPrefix + ".queue_size", getQueue().size());
        } finally {
            super.afterExecute(r, t);
        }
    }

    @Override
    protected void terminated() {
        try {
            metrics.incrementCounter(metricsPrefix + ".terminated");
            metrics.setGauge(metricsPrefix + ".active_count", 0);
            metrics.setGauge(metricsPrefix + ".pool_size", 0);
        } finally {
            super.terminated();
        }
    }

    /**
     * Submit a task with metrics tracking
     *
     * @param <T> the result type
     * @param task the task to submit
     * @param taskName the name of the task for metrics
     * @return a future representing the task
     */
    public <T> Future<T> submitWithMetrics(Callable<T> task, String taskName) {
        Metrics taskMetrics = MetricsRegistry.getInstance().getOrCreateMetrics(taskName);
        return submit(() -> {
            long startTime = System.currentTimeMillis();
            taskMetrics.incrementCounter("executions");
            try {
                T result = task.call();
                taskMetrics.incrementCounter("successes");
                return result;
            } catch (Exception e) {
                taskMetrics.incrementCounter("failures");
                if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                } else {
                    throw new ExecutionException(e);
                }
            } finally {
                long endTime = System.currentTimeMillis();
                taskMetrics.recordTimer("execution_time", endTime - startTime);
            }
        });
    }

    /**
     * Execute a runnable task with metrics tracking
     *
     * @param command the task to execute
     * @param taskName the name of the task for metrics
     */
    public void executeWithMetrics(Runnable command, String taskName) {
        Metrics taskMetrics = MetricsRegistry.getInstance().getOrCreateMetrics(taskName);
        execute(() -> {
            long startTime = System.currentTimeMillis();
            taskMetrics.incrementCounter("executions");
            try {
                command.run();
                taskMetrics.incrementCounter("successes");
            } catch (Exception e) {
                taskMetrics.incrementCounter("failures");
                throw e;
            } finally {
                long endTime = System.currentTimeMillis();
                taskMetrics.recordTimer("execution_time", endTime - startTime);
            }
        });
    }

    /**
     * Execute a supplier function with metrics tracking and retry logic
     *
     * @param <T> the result type
     * @param supplier the supplier function
     * @param taskName the name of the task for metrics
     * @param maxRetries the maximum number of retries
     * @param retryDelayMs the delay between retries in milliseconds
     * @return the result of the supplier function
     * @throws ExecutionException if the supplier function throws an exception
     */
    public <T> T executeWithRetry(Supplier<T> supplier, String taskName, int maxRetries, long retryDelayMs)
            throws ExecutionException {
        Metrics taskMetrics = MetricsRegistry.getInstance().getOrCreateMetrics(taskName);

        int attempts = 0;
        Exception lastException = null;

        while (attempts <= maxRetries) {
            attempts++;
            taskMetrics.incrementCounter("attempts");

            try {
                long startTime = System.currentTimeMillis();
                T result = supplier.get();
                taskMetrics.incrementCounter("successes");
                taskMetrics.recordTimer("execution_time", System.currentTimeMillis() - startTime);

                return result;
            } catch (Exception e) {
                lastException = e;
                taskMetrics.incrementCounter("failures");

                if (attempts <= maxRetries) {
                    taskMetrics.incrementCounter("retries");
                    try {
                        Thread.sleep(retryDelayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new ExecutionException("Retry interrupted", ie);
                    }
                }
            }
        }

        throw new ExecutionException("Failed after " + attempts + " attempts", lastException);
    }

    /**
     * Get the metrics for this thread pool
     *
     * @return the metrics
     */
    public Metrics getMetrics() {
        return metrics;
    }
}