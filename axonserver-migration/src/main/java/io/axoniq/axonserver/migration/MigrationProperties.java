package io.axoniq.axonserver.migration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author Marc Gathier
 */
@ConfigurationProperties("axon.serializer")
@Configuration
public class MigrationProperties {
    private SerializerType events = SerializerType.DEFAULT;

    public SerializerType getEvents() {
        return events;
    }

    public void setEvents(SerializerType events) {
        this.events = events;
    }
}
