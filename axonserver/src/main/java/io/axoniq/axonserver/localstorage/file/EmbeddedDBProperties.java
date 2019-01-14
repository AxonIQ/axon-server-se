package io.axoniq.axonserver.localstorage.file;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;

/**
 * @author Marc Gathier
 */
@Configuration
@ConfigurationProperties(prefix = "axoniq.axonserver")
public class EmbeddedDBProperties {

    @NestedConfigurationProperty
    private StorageProperties event = new StorageProperties();

    @NestedConfigurationProperty
    private StorageProperties snapshot = new StorageProperties(".snapshots", ".sindex", ".sbloom");

    @PostConstruct
    public void init() {
        if(snapshot.getStorage() == null) {
            snapshot.setStorage(event.getStorage());
        }
    }

    public StorageProperties getEvent() {
        return event;
    }

    public void setEvent(StorageProperties event) {
        this.event = event;
    }

    public StorageProperties getSnapshot() {
        return snapshot;
    }

    public void setSnapshot(StorageProperties snapshot) {
        this.snapshot = snapshot;
    }
}
