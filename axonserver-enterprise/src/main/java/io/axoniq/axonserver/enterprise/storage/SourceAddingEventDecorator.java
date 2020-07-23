package io.axoniq.axonserver.enterprise.storage;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.localstorage.EventDecorator;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.SerializedEventWithToken;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;

/**
 * Event decorator that adds a meta data entry to events containing the node where the event was read.
 * Only created when profile "testing-only" is enabled.
 *
 * @author Marc Gathier
 * @since 4.4
 */
@Component
@Profile("testing-only")
public class SourceAddingEventDecorator implements EventDecorator {

    public static final String AXONSERVER_SOURCE = "axonserver.source";
    private final MetaDataValue name;

    public SourceAddingEventDecorator(MessagingPlatformConfiguration messagingPlatformConfiguration) {
        name = MetaDataValue.newBuilder().setTextValue(messagingPlatformConfiguration.getName()).build();
    }

    @Override
    public SerializedEvent decorateEvent(SerializedEvent serializedEvent) {
        Event event = Event.newBuilder(serializedEvent.asEvent())
                           .putMetaData(AXONSERVER_SOURCE, name)
                           .build();

        return new SerializedEvent(event);
    }

    @Override
    public InputStream decorateEventWithToken(InputStream inputStream) {
        try {
            EventWithToken eventWithToken = EventWithToken.parseFrom(inputStream);
            Event event = Event.newBuilder(eventWithToken.getEvent())
                               .putMetaData(AXONSERVER_SOURCE, name)
                               .build();
            return new SerializedEventWithToken(eventWithToken.getToken(), event).asInputStream();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public EventWithToken decorateEventWithToken(EventWithToken eventWithToken) {
        Event event = Event.newBuilder(eventWithToken.getEvent())
                           .putMetaData(AXONSERVER_SOURCE, name)
                           .build();
        return EventWithToken.newBuilder().setEvent(event).setToken(eventWithToken.getToken()).build();
    }
}
