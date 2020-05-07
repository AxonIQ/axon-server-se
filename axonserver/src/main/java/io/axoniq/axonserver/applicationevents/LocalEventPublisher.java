package io.axoniq.axonserver.applicationevents;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;

/**
 * Publish the internal events to the listeners registered locally, in the current Axon Server instance.
 * This is the default implementation of {@link AxonServerEventPublisher} when using the Standard Edition.
 *
 * @author Sara Pellegrini
 * @since 4.4
 */
@Component
public class LocalEventPublisher implements AxonServerEventPublisher {

    private final ApplicationEventPublisher eventPublisher;

    public LocalEventPublisher(@Qualifier("applicationEventPublisher") ApplicationEventPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
    }

    /**
     * Notify all matching listeners registered in the current local instance of AxonServer
     * with this internal event type.
     *
     * @param event the event to publish
     */
    @Override
    public void publishEvent(@Nonnull Object event) {
        this.eventPublisher.publishEvent(event);
    }
}
