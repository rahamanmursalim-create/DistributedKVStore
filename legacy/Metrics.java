package com.distributedkv.common;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Metrics collection for monitoring and statistics
 */
public class Metrics {
    private final String name;

    // Counter metrics use LongAdder for better concurrency
    private final Map<String, LongAdder> counters = new ConcurrentHashMap<>();

    // Gauge metrics use AtomicLong for set operations
    private final Map<String, AtomicLong> gauges = new ConcurrentHashMap<>();

    // Timer metrics track count, sum, min, max, and sum of squares for statistics
    private final Map<String, TimerMetric> timers = new ConcurrentHashMap<>();

    // Histogram metrics track distribution of values
    private final Map<String, HistogramMetric> histograms = new ConcurrentHashMap<>();

    /**
     * Create metrics with a name
     *
     * @param name the name of this metrics collection
     */
    public Metrics(String name) {
        this.name = name;
    }

    /**
     * Get the name of this metrics collection
     *
     * @return metrics name
     */
    public String getName() {
        return name;
    }

    // ---------------------- Counter Operations ----------------------

    /**
     * Increment a counter by 1
     *
     * @param key the counter name
     */
    public void incrementCounter(String key) {
        incrementCounter(key, 1);
    }

    /**
     * Increment a counter by a specified amount
     *
     * @param key the counter name
     * @param amount the amount to increment
     */
    public void incrementCounter(String key, long amount) {
        counters.computeIfAbsent(key, k -> new LongAdder()).add(amount);
    }

    /**
     * Decrement a counter by 1
     *
     * @param key the counter name
     */
    public void decrementCounter(String key) {
        decrementCounter(key, 1);
    }

    /**
     * Decrement a counter by a specified amount
     *
     * @param key the counter name
     * @param amount the amount to decrement
     */
    public void decrementCounter(String key, long amount) {
        counters.computeIfAbsent(key, k -> new LongAdder()).add(-amount);
    }

    /**
     * Get the current value of a counter
     *
     * @param key the counter name
     * @return the current value, or 0 if the counter doesn't exist
     */
    public long getCounter(String key) {
        LongAdder counter = counters.get(key);
        return counter != null ? counter.sum() : 0;
    }

    /**
     * Get all counters
     *
     * @return a map of counter names to values
     */
    public Map<String, Long> getCounters() {
        Map<String, Long> result = new ConcurrentHashMap<>();
        counters.forEach((k, v) -> result.put(k, v.sum()));
        return result;
    }

    // ---------------------- Gauge Operations ----------------------

    /**
     * Set a gauge to a value
     *
     * @param key the gauge name
     * @param value the value to set
     */
    public void setGauge(String key, long value) {
        gauges.computeIfAbsent(key, k -> new AtomicLong(0)).set(value);
    }

    /**
     * Get the current value of a gauge
     *
     * @param key the gauge name
     * @return the current value, or 0 if the gauge doesn't exist
     */
    public long getGauge(String key) {
        AtomicLong gauge = gauges.get(key);
        return gauge != null ? gauge.get() : 0;
    }

    /**
     * Get all gauges
     *
     * @return a map of gauge names to values
     */
    public Map<String, Long> getGauges() {
        Map<String, Long> result = new ConcurrentHashMap<>();
        gauges.forEach((k, v) -> result.put(k, v.get()));
        return result;
    }

    /**
     * Increment a gauge by 1
     *
     * @param key the gauge name
     * @return the new value
     */
    public long incrementGauge(String key) {
        return incrementGauge(key, 1);
    }

    /**
     * Increment a gauge by a specified amount
     *
     * @param key the gauge name
     * @param amount the amount to increment
     * @return the new value
     */
    public long incrementGauge(String key, long amount) {
        return gauges.computeIfAbsent(key, k -> new AtomicLong(0)).addAndGet(amount);
    }

    /**
     * Decrement a gauge by 1
     *
     * @param key the gauge name
     * @return the new value
     */
    public long decrementGauge(String key) {
        return decrementGauge(key, 1);
    }

    /**
     * Decrement a gauge by a specified amount
     *
     * @param key the gauge name
     * @param amount the amount to decrement
     * @return the new value
     */
    public long decrementGauge(String key, long amount) {
        return gauges.computeIfAbsent(key, k -> new AtomicLong(0)).addAndGet(-amount);
    }

    // ---------------------- Timer Operations ----------------------

    /**
     * Record a timed operation
     *
     * @param key the timer name
     * @param durationMs the duration in milliseconds
     */
    public void recordTimer(String key, long durationMs) {
        timers.computeIfAbsent(key, k -> new TimerMetric()).record(durationMs);
    }

    /**
     * Get timer statistics
     *
     * @param key the timer name
     * @return timer statistics, or null if timer doesn't exist
     */
    public TimerStats getTimerStats(String key) {
        TimerMetric timer = timers.get(key);
        return timer != null ? timer.getStats() : null;
    }

    /**
     * Get all timer statistics
     *
     * @return a map of timer names to timer statistics
     */
    public Map<String, TimerStats> getTimerStats() {
        Map<String, TimerStats> result = new ConcurrentHashMap<>();
        timers.forEach((k, v) -> result.put(k, v.getStats()));
        return result;
    }

    /**
     * Reset a timer
     *
     * @param key the timer name
     */
    public void resetTimer(String key) {
        TimerMetric timer = timers.get(key);
        if (timer != null) {
            timer.reset();
        }
    }

    // ---------------------- Histogram Operations ----------------------

    /**
     * Record a value in a histogram
     *
     * @param key the histogram name
     * @param value the value to record
     */
    public void recordHistogram(String key, long value) {
        histograms.computeIfAbsent(key, k -> new HistogramMetric()).record(value);
    }

    /**
     * Get histogram statistics
     *
     * @param key the histogram name
     * @return histogram statistics, or null if histogram doesn't exist
     */
    public HistogramStats getHistogramStats(String key) {
        HistogramMetric histogram = histograms.get(key);
        return histogram != null ? histogram.getStats() : null;
    }

    /**
     * Get all histogram statistics
     *
     * @return a map of histogram names to histogram statistics
     */
    public Map<String, HistogramStats> getHistogramStats() {
        Map<String, HistogramStats> result = new ConcurrentHashMap<>();
        histograms.forEach((k, v) -> result.put(k, v.getStats()));
        return result;
    }

    /**
     * Reset a histogram
     *
     * @param key the histogram name
     */
    public void resetHistogram(String key) {
        HistogramMetric histogram = histograms.get(key);
        if (histogram != null) {
            histogram.reset();
        }
    }

    // ---------------------- Utility Operations ----------------------

    /**
     * Reset all metrics
     */
    public void reset() {
        counters.clear();
        gauges.clear();
        timers.clear();
        histograms.clear();
    }

    /**
     * Get all metrics as a JSON-compatible map
     *
     * @return a map of all metrics
     */
    public Map<String, Object> getAllMetrics() {
        Map<String, Object> result = new ConcurrentHashMap<>();

        Map<String, Long> counterMap = getCounters();
        if (!counterMap.isEmpty()) {
            result.put("counters", counterMap);
        }

        Map<String, Long> gaugeMap = getGauges();
        if (!gaugeMap.isEmpty()) {
            result.put("gauges", gaugeMap);
        }

        Map<String, TimerStats> timerMap = getTimerStats();
        if (!timerMap.isEmpty()) {
            result.put("timers", timerMap);
        }

        Map<String, HistogramStats> histogramMap = getHistogramStats();
        if (!histogramMap.isEmpty()) {
            result.put("histograms", histogramMap);
        }

        return result;
    }

    // ---------------------- Inner Classes ----------------------

    /**
     * Timer metric that tracks count, sum, min, max, and sum of squares
     */
    private static class TimerMetric {
        private final LongAdder count = new LongAdder();
        private final LongAdder sum = new LongAdder();
        private final AtomicLong min = new AtomicLong(Long.MAX_VALUE);
        private final AtomicLong max = new AtomicLong(Long.MIN_VALUE);
        private final LongAdder sumSquares = new LongAdder();

        public void record(long value) {
            count.increment();
            sum.add(value);
            sumSquares.add(value * value);

            // Update min atomically
            long currentMin;
            do {
                currentMin = min.get();
                if (value >= currentMin) {
                    break;
                }
            } while (!min.compareAndSet(currentMin, value));

            // Update max atomically
            long currentMax;
            do {
                currentMax = max.get();
                if (value <= currentMax) {
                    break;
                }
            } while (!max.compareAndSet(currentMax, value));
        }

        public void reset() {
            count.reset();
            sum.reset();
            min.set(Long.MAX_VALUE);
            max.set(Long.MIN_VALUE);
            sumSquares.reset();
        }

        public TimerStats getStats() {
            long countValue = count.sum();
            if (countValue == 0) {
                return new TimerStats(0, 0, 0, 0, 0);
            }

            long sumValue = sum.sum();
            long minValue = min.get();
            long maxValue = max.get();
            long sumSquaresValue = sumSquares.sum();

            // Calculate average and standard deviation
            double avg = (double) sumValue / countValue;
            double variance = ((double) sumSquaresValue / countValue) - (avg * avg);
            double stdDev = Math.sqrt(variance);

            return new TimerStats(countValue, avg, minValue, maxValue, stdDev);
        }
    }

    /**
     * Timer statistics
     */
    public static class TimerStats {
        private final long count;
        private final double average;
        private final long min;
        private final long max;
        private final double stdDev;

        public TimerStats(long count, double average, long min, long max, double stdDev) {
            this.count = count;
            this.average = average;
            this.min = min;
            this.max = max;
            this.stdDev = stdDev;
        }

        public long getCount() {
            return count;
        }

        public double getAverage() {
            return average;
        }

        public long getMin() {
            return min;
        }

        public long getMax() {
            return max;
        }

        public double getStdDev() {
            return stdDev;
        }

        @Override
        public String toString() {
            return "TimerStats{" +
                    "count=" + count +
                    ", average=" + average +
                    ", min=" + min +
                    ", max=" + max +
                    ", stdDev=" + stdDev +
                    '}';
        }
    }

    /**
     * Histogram metric that tracks value distribution
     */
    private static class HistogramMetric {
        private final TimerMetric statistics = new TimerMetric();
        private final ConcurrentHashMap<Long, LongAdder> buckets = new ConcurrentHashMap<>();

        public void record(long value) {
            statistics.record(value);
            buckets.computeIfAbsent(value, k -> new LongAdder()).increment();
        }

        public void reset() {
            statistics.reset();
            buckets.clear();
        }

        public HistogramStats getStats() {
            Map<Long, Long> bucketCounts = new ConcurrentHashMap<>();
            buckets.forEach((k, v) -> bucketCounts.put(k, v.sum()));

            return new HistogramStats(statistics.getStats(), bucketCounts);
        }
    }

    /**
     * Histogram statistics
     */
    public static class HistogramStats {
        private final TimerStats statistics;
        private final Map<Long, Long> buckets;

        public HistogramStats(TimerStats statistics, Map<Long, Long> buckets) {
            this.statistics = statistics;
            this.buckets = buckets;
        }

        public TimerStats getStatistics() {
            return statistics;
        }

        public Map<Long, Long> getBuckets() {
            return buckets;
        }

        @Override
        public String toString() {
            return "HistogramStats{" +
                    "statistics=" + statistics +
                    ", buckets=" + buckets +
                    '}';
        }
    }
}