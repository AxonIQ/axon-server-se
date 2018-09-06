package io.axoniq.axonhub.modules.advancedstorage;

import io.axoniq.axonhub.localstorage.file.StorageProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.context.annotation.Configuration;

/**
 * Author: marc
 */
@ConfigurationProperties(prefix = "axoniq.axonhub.advancedstorage")
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
