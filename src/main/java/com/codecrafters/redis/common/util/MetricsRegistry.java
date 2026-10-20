package com.distributedkv.common;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Registry for metrics collection that provides a centralized way to access metrics
 * across the system
 */
public class MetricsRegistry {
    // Singleton instance
    private static final MetricsRegistry INSTANCE = new MetricsRegistry();

    // Map of metric names to metrics instances
    private final Map<String, Metrics> metricsMap = new ConcurrentHashMap<>();

    // Map of metric prefixes to suppliers
    private final Map<String, Supplier<Map<String, Object>>> metricSuppliers = new ConcurrentHashMap<>();

    /**
     * Private constructor for singleton
     */
    private MetricsRegistry() {
        // Private constructor to enforce singleton
    }

    /**
     * Get the singleton instance
     *
     * @return the singleton instance
     */
    public static MetricsRegistry getInstance() {
        return INSTANCE;
    }

    /**
     * Get or create a metrics instance
     *
     * @param name the metrics name
     * @return the metrics instance
     */
    public Metrics getOrCreateMetrics(String name) {
        return metricsMap.computeIfAbsent(name, Metrics::new);
    }

    /**
     * Get a metrics instance if it exists
     *
     * @param name the metrics name
     * @return the metrics instance, or null if it doesn't exist
     */
    public Metrics getMetrics(String name) {
        return metricsMap.get(name);
    }

    /**
     * Register a metrics instance
     *
     * @param metrics the metrics instance
     */
    public void registerMetrics(Metrics metrics) {
        metricsMap.put(metrics.getName(), metrics);
    }

    /**
     * Unregister a metrics instance
     *
     * @param name the metrics name
     * @return the unregistered metrics, or null if it didn't exist
     */
    public Metrics unregisterMetrics(String name) {
        return metricsMap.remove(name);
    }

    /**
     * Register a dynamic metrics supplier
     *
     * @param prefix the metrics prefix
     * @param supplier the supplier of metrics
     */
    public void registerMetricsSupplier(String prefix, Supplier<Map<String, Object>> supplier) {
        metricSuppliers.put(prefix, supplier);
    }

    /**
     * Unregister a metrics supplier
     *
     * @param prefix the metrics prefix
     */
    public void unregisterMetricsSupplier(String prefix) {
        metricSuppliers.remove(prefix);
    }

    /**
     * Clear all metrics
     */
    public void clear() {
        metricsMap.clear();
        metricSuppliers.clear();
    }

    /**
     * Get all metrics instances
     *
     * @return a map of metrics names to metrics instances
     */
    public Map<String, Metrics> getAllMetrics() {
        return new ConcurrentHashMap<>(metricsMap);
    }

    /**
     * Get all metrics as a JSON-compatible map
     *
     * @return a map of all metrics
     */
    public Map<String, Object> getAllMetricsData() {
        Map<String, Object> result = new ConcurrentHashMap<>();

        // Add static metrics
        metricsMap.forEach((name, metrics) -> {
            Map<String, Object> metricsData = metrics.getAllMetrics();
            if (!metricsData.isEmpty()) {
                result.put(name, metricsData);
            }
        });

        // Add dynamic metrics
        metricSuppliers.forEach((prefix, supplier) -> {
            Map<String, Object> dynamicMetrics = supplier.get();
            if (dynamicMetrics != null && !dynamicMetrics.isEmpty()) {
                for (Map.Entry<String, Object> entry : dynamicMetrics.entrySet()) {
                    result.put(prefix + "." + entry.getKey(), entry.getValue());
                }
            }
        });

        return result;
    }

    /**
     * Convenience method to increment a counter in a named metrics instance
     *
     * @param metricsName the metrics name
     * @param counterName the counter name
     */
    public void incrementCounter(String metricsName, String counterName) {
        getOrCreateMetrics(metricsName).incrementCounter(counterName);
    }

    /**
     * Convenience method to increment a counter by a specified amount
     *
     * @param metricsName the metrics name
     * @param counterName the counter name
     * @param amount the amount to increment
     */
    public void incrementCounter(String metricsName, String counterName, long amount) {
        getOrCreateMetrics(metricsName).incrementCounter(counterName, amount);
    }

    /**
     * Convenience method to set a gauge in a named metrics instance
     *
     * @param metricsName the metrics name
     * @param gaugeName the gauge name
     * @param value the value to set
     */
    public void setGauge(String metricsName, String gaugeName, long value) {
        getOrCreateMetrics(metricsName).setGauge(gaugeName, value);
    }

    /**
     * Convenience method to record a timer in a named metrics instance
     *
     * @param metricsName the metrics name
     * @param timerName the timer name
     * @param durationMs the duration in milliseconds
     */
    public void recordTimer(String metricsName, String timerName, long durationMs) {
        getOrCreateMetrics(metricsName).recordTimer(timerName, durationMs);
    }

    /**
     * Convenience method to record a histogram value in a named metrics instance
     *
     * @param metricsName the metrics name
     * @param histogramName the histogram name
     * @param value the value to record
     */
    public void recordHistogram(String metricsName, String histogramName, long value) {
        getOrCreateMetrics(metricsName).recordHistogram(histogramName, value);
    }

    /**
     * Get the combined metrics count (number of registered metrics)
     *
     * @return the total number of metrics
     */
    public int getMetricsCount() {
        return metricsMap.size();
    }
}