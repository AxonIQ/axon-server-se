package io.axoniq.axonserver.enterprise.config;


import io.axoniq.axonserver.cluster.replication.file.StorageProperties;
import io.axoniq.axonserver.config.SystemInfoProvider;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Author: marc
 */
@ConfigurationProperties(prefix = "axoniq.axonserver.replication")
@Configuration
public class RaftProperties extends StorageProperties {
    private int minElectionTimeout = 1000;
    private int maxElectionTimeout = 2500;
    private int heartbeatTimeout = 100;
    private int maxEntriesPerBatch = 100;
    private int flowBuffer = 1000;

    private final SystemInfoProvider systemInfoProvider;
    private int maxReplicationRound = 10;
    private boolean logCompactionEnabled = true;
    private int logRetentionHours = 1;

    public RaftProperties(SystemInfoProvider systemInfoProvider) {
        this.systemInfoProvider = systemInfoProvider;
    }

    public int getMinElectionTimeout() {
        return minElectionTimeout;
    }

    public void setMinElectionTimeout(int minElectionTimeout) {
        this.minElectionTimeout = minElectionTimeout;
    }

    public int getMaxElectionTimeout() {
        return maxElectionTimeout;
    }

    public void setMaxElectionTimeout(int maxElectionTimeout) {
        this.maxElectionTimeout = maxElectionTimeout;
    }

    public int getHeartbeatTimeout() {
        return heartbeatTimeout;
    }

    public void setHeartbeatTimeout(int heartbeatTimeout) {
        this.heartbeatTimeout = heartbeatTimeout;
    }

    public int getMaxEntriesPerBatch() {
        return maxEntriesPerBatch;
    }

    public void setMaxEntriesPerBatch(int maxEntriesPerBatch) {
        this.maxEntriesPerBatch = maxEntriesPerBatch;
    }

    public int getFlowBuffer() {
        return flowBuffer;
    }

    public void setFlowBuffer(int flowBuffer) {
        this.flowBuffer = flowBuffer;
    }

    @Override
    public boolean isCleanerHackEnabled() {
        return systemInfoProvider.javaOnWindows() && ! systemInfoProvider.javaWithModules();
    }

    @Override
    public boolean isUseMmapIndex() {
        return ! (systemInfoProvider.javaOnWindows() && systemInfoProvider.javaWithModules());
    }

    public int getMaxReplicationRound() {
        return maxReplicationRound;
    }

    public void setMaxReplicationRound(int maxReplicationRound) {
        this.maxReplicationRound = maxReplicationRound;
    }

    public boolean isLogCompactionEnabled() {
        return logCompactionEnabled;
    }

    public void setLogCompactionEnabled(boolean logCompactionEnabled) {
        this.logCompactionEnabled = logCompactionEnabled;
    }

    public int getLogRetentionHours() {
        return logRetentionHours;
    }

    public void setLogRetentionHours(int logRetentionHours) {
        this.logRetentionHours = Math.max(logRetentionHours, 1);
    }
}
