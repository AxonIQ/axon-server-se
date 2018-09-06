package io.axoniq.axonhub.config;

import io.axoniq.axonhub.KeepNames;

/**
 * Author: marc
 */
@KeepNames
public class ClusterConfiguration {
    /**
     * Cluster enabled
     */
    private boolean enabled;
    /**
     * Delay before the first run of the connection checker (in ms.)
     */
    private long connectionCheckDelay = 1000;
    /**
     * Delay before the first run of the metrics distributor (in ms.)
     */
    private long metricsDistributeDelay = 1000;
    /**
     * Delay before the first run of the rebalancer (in seconds)
     */
    private long rebalanceDelay = 7;
    /**
     * Interval between each run of the connection checker (in ms.)
     */
    private long connectionCheckInterval = 1000;
    /**
     * Interval between each run of the metrics distributor (in ms.)
     */
    private long metricsDistributeInterval = 1000;
    /**
     * Interval between each run of the rebalancer (in seconds)
     */
    private long rebalanceInterval = 15;

    /**
     * Timeout for connection request (in ms.)
     */
    private long connectionWaitTime = 3000;

    public long getConnectionCheckDelay() {
        return connectionCheckDelay;
    }

    public long getMetricsDistributeDelay() {
        return metricsDistributeDelay;
    }

    public long getConnectionCheckInterval() {
        return connectionCheckInterval;
    }

    public long getMetricsDistributeInterval() {
        return metricsDistributeInterval;
    }

    public void setConnectionCheckDelay(long connectionCheckDelay) {
        this.connectionCheckDelay = connectionCheckDelay;
    }

    public void setMetricsDistributeDelay(long metricsDistributeDelay) {
        this.metricsDistributeDelay = metricsDistributeDelay;
    }

    public void setConnectionCheckInterval(long connectionCheckInterval) {
        this.connectionCheckInterval = connectionCheckInterval;
    }

    public void setMetricsDistributeInterval(long metricsDistributeInterval) {
        this.metricsDistributeInterval = metricsDistributeInterval;
    }

    public long getRebalanceInterval() {
        return rebalanceInterval;
    }

    public long getRebalanceDelay() {
        return rebalanceDelay;
    }

    public void setRebalanceInterval(long rebalanceInterval) {
        this.rebalanceInterval = rebalanceInterval;
    }

    public void setRebalanceDelay(long rebalanceDelay) {
        this.rebalanceDelay = rebalanceDelay;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public long getConnectionWaitTime() {
        return connectionWaitTime;
    }

    public void setConnectionWaitTime(long connectionWaitTime) {
        this.connectionWaitTime = connectionWaitTime;
    }
}
