package io.axoniq.axonserver.enterprise.config;


import io.axoniq.axonserver.cluster.replication.file.StorageProperties;
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
    private int heartbeatTimeout = 250;

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
}
