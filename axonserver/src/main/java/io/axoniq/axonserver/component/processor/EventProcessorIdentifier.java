package io.axoniq.axonserver.component.processor;

import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorId;
import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import io.axoniq.axonserver.component.processor.listener.ClientProcessor;

import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * Identifies uniquely an event processor inside a specific context.
 *
 * @author Sara Pellegrini
 * @since 4.4
 */
public final class EventProcessorIdentifier implements EventProcessorId {

    private final String name;

    private final String tokenStoreIdentifier;

    public EventProcessorIdentifier(ClientProcessor clientProcessor) {
        this(clientProcessor.eventProcessorInfo().getProcessorName(),
             clientProcessor.eventProcessorInfo().getTokenStoreIdentifier());
    }

    public EventProcessorIdentifier(TrackingEventProcessor eventProcessor) {
        this(eventProcessor.name(), eventProcessor.tokenStoreIdentifier());
    }

    public EventProcessorIdentifier(String name, String tokenStoreIdentifier) {
        this.name = name;
        this.tokenStoreIdentifier = tokenStoreIdentifier;
    }

    @Nonnull
    public String name() {
        return name;
    }

    @Nonnull
    public String tokenStoreIdentifier() {
        return tokenStoreIdentifier;
    }

    public boolean equals(EventProcessorId id) {
        return Objects.equals(name, id.name()) &&
                Objects.equals(tokenStoreIdentifier, id.tokenStoreIdentifier());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EventProcessorIdentifier that = (EventProcessorIdentifier) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(tokenStoreIdentifier, that.tokenStoreIdentifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, tokenStoreIdentifier);
    }

    @Override
    public String toString() {
        return "EventProcessorIdentifier{" +
                "name='" + name + '\'' +
                ", tokenStoreIdentifier='" + tokenStoreIdentifier + '\'' +
                '}';
    }
}
