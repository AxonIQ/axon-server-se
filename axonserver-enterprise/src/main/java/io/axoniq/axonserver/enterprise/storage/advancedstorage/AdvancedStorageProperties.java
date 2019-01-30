package io.axoniq.axonserver.enterprise.storage.advancedstorage;

import io.axoniq.axonserver.config.SystemInfoProvider;
import io.axoniq.axonserver.localstorage.file.StorageProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.context.annotation.Configuration;

/**
 * Author: marc
 */
@ConfigurationProperties(prefix = "axoniq.axonserver.advancedstorage")
@Configuration
public class AdvancedStorageProperties {

    @NestedConfigurationProperty
    private StorageProperties eventSecondary;

    public StorageProperties getEventSecondary() {
        return eventSecondary;
    }

    public void setEventSecondary(StorageProperties eventSecondary) {
        this.eventSecondary = eventSecondary;
    }

    public AdvancedStorageProperties(SystemInfoProvider systemInfoProvider) {
        eventSecondary = new StorageProperties(systemInfoProvider);
    }
}
