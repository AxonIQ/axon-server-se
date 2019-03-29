package io.axoniq.axonserver.enterprise.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration properties used for setting up cluster of AxonServer instances.
 *
 * @author Marc Gathier
 */
@Configuration
@ConfigurationProperties(prefix = "axoniq.axonserver.cluster")
public class ClusterConfiguration {
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

    @NestedConfigurationProperty
    private FlowControl commandFlowControl = new FlowControl();

    @NestedConfigurationProperty
    private FlowControl queryFlowControl = new FlowControl();



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

    public long getConnectionWaitTime() {
        return connectionWaitTime;
    }

    public void setConnectionWaitTime(long connectionWaitTime) {
        this.connectionWaitTime = connectionWaitTime;
    }

    public FlowControl getCommandFlowControl() {
        return commandFlowControl;
    }

    public void setCommandFlowControl(FlowControl commandFlowControl) {
        this.commandFlowControl = commandFlowControl;
    }

    public FlowControl getQueryFlowControl() {
        return queryFlowControl;
    }

    public void setQueryFlowControl(FlowControl queryFlowControl) {
        this.queryFlowControl = queryFlowControl;
    }
}
