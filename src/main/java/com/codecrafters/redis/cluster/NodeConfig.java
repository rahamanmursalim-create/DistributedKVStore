package com.distributedkv.config;

import java.util.Properties;

/**
 * Configuration for a node in the distributed KV system
 */
public class NodeConfig {
    private final String nodeId;
    private final String host;
    private final int port;
    private final String dataDir;
    private final int threadPoolSize;
    private final int networkTimeout;
    private final int maxConnections;
    private final int bufferSize;
    private final boolean tcpNoDelay;
    private final boolean reuseAddress;
    private final int networkWorkers;
    private final int ioWorkers;
    private final int applyWorkers;
    private final String logLevel;
    private final String logFile;

    /**
     * Constructs a NodeConfig from properties
     *
     * @param properties the configuration properties
     */
    public NodeConfig(Properties properties) {
        this.nodeId = properties.getProperty("node.id", "node1");
        this.host = properties.getProperty("node.host", "localhost");
        this.port = Integer.parseInt(properties.getProperty("node.port", "10000"));
        this.dataDir = properties.getProperty("node.data.dir", "data");
        this.threadPoolSize = Integer.parseInt(properties.getProperty("node.thread.pool.size", "32"));

        // Network configuration
        this.networkTimeout = Integer.parseInt(properties.getProperty("network.timeout.ms", "5000"));
        this.maxConnections = Integer.parseInt(properties.getProperty("network.max.connections", "1000"));
        this.bufferSize = Integer.parseInt(properties.getProperty("network.buffer.size", "8192"));
        this.tcpNoDelay = Boolean.parseBoolean(properties.getProperty("network.tcp.no.delay", "true"));
        this.reuseAddress = Boolean.parseBoolean(properties.getProperty("network.reuse.address", "true"));

        // Thread pool configuration
        this.networkWorkers = Integer.parseInt(properties.getProperty("performance.network.workers", "4"));
        this.ioWorkers = Integer.parseInt(properties.getProperty("performance.io.workers", "8"));
        this.applyWorkers = Integer.parseInt(properties.getProperty("performance.apply.workers", "4"));

        // Logging configuration
        this.logLevel = properties.getProperty("logging.level", "INFO");
        this.logFile = properties.getProperty("logging.file", "logs/server.log");
    }

    /**
     * Gets the node ID
     *
     * @return node ID
     */
    public String getNodeId() {
        return nodeId;
    }

    /**
     * Gets the host name or IP address
     *
     * @return host
     */
    public String getHost() {
        return host;
    }

    /**
     * Gets the port number
     *
     * @return port
     */
    public int getPort() {
        return port;
    }

    /**
     * Gets the data directory path
     *
     * @return data directory
     */
    public String getDataDir() {
        return dataDir;
    }

    /**
     * Gets the thread pool size
     *
     * @return thread pool size
     */
    public int getThreadPoolSize() {
        return threadPoolSize;
    }

    /**
     * Gets the network timeout in milliseconds
     *
     * @return network timeout
     */
    public int getNetworkTimeout() {
        return networkTimeout;
    }

    /**
     * Gets the maximum number of connections
     *
     * @return max connections
     */
    public int getMaxConnections() {
        return maxConnections;
    }

    /**
     * Gets the network buffer size
     *
     * @return buffer size
     */
    public int getBufferSize() {
        return bufferSize;
    }

    /**
     * Checks if TCP no delay is enabled
     *
     * @return true if TCP no delay is enabled
     */
    public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    /**
     * Checks if address reuse is enabled
     *
     * @return true if address reuse is enabled
     */
    public boolean isReuseAddress() {
        return reuseAddress;
    }

    /**
     * Gets the number of network worker threads
     *
     * @return network workers count
     */
    public int getNetworkWorkers() {
        return networkWorkers;
    }

    /**
     * Gets the number of IO worker threads
     *
     * @return IO workers count
     */
    public int getIoWorkers() {
        return ioWorkers;
    }

    /**
     * Gets the number of apply worker threads
     *
     * @return apply workers count
     */
    public int getApplyWorkers() {
        return applyWorkers;
    }

    /**
     * Gets the log level
     *
     * @return log level
     */
    public String getLogLevel() {
        return logLevel;
    }

    /**
     * Gets the log file path
     *
     * @return log file
     */
    public String getLogFile() {
        return logFile;
    }

    /**
     * Gets the node address in the format host:port
     *
     * @return node address
     */
    public String getNodeAddress() {
        return host + ":" + port;
    }

    @Override
    public String toString() {
        return "NodeConfig{" +
                "nodeId='" + nodeId + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", dataDir='" + dataDir + '\'' +
                ", threadPoolSize=" + threadPoolSize +
                ", networkTimeout=" + networkTimeout +
                ", maxConnections=" + maxConnections +
                ", bufferSize=" + bufferSize +
                ", tcpNoDelay=" + tcpNoDelay +
                ", reuseAddress=" + reuseAddress +
                ", networkWorkers=" + networkWorkers +
                ", ioWorkers=" + ioWorkers +
                ", applyWorkers=" + applyWorkers +
                ", logLevel='" + logLevel + '\'' +
                ", logFile='" + logFile + '\'' +
                '}';
    }
}