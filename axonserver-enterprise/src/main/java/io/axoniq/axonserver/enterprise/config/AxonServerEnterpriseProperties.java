package io.axoniq.axonserver.enterprise.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 *
 *
 * @author Sara Pellegrini
 * @since 4.1
 */
@Component
@ConfigurationProperties(prefix = "axoniq.axonserver.enterprise")
public class AxonServerEnterpriseProperties {

    private long contextConfigurationSyncRate;

    public long contextConfigurationSyncRate() {
        return contextConfigurationSyncRate;
    }

    public void setContextConfigurationSyncRate(long contextConfigurationSyncRate) {
        this.contextConfigurationSyncRate = contextConfigurationSyncRate;
    }
}

