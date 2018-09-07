package io.axoniq.axonserver.modules.advancedstorage;

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
    private StorageProperties eventSecondary = new StorageProperties();

    public StorageProperties getEventSecondary() {
        return eventSecondary;
    }

    public void setEventSecondary(StorageProperties eventSecondary) {
        this.eventSecondary = eventSecondary;
    }
}
