package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import io.axoniq.axonserver.eventstore.transformation.transformation.active.TransformationResources;
import io.axoniq.axonserver.grpc.event.Event;
import reactor.core.publisher.Mono;

public class ContextTransformationResources implements TransformationResources {

    private final EventProvider eventProvider;

    public ContextTransformationResources(EventProvider eventProvider) {
        this.eventProvider = eventProvider;
    }

    @Override
    public Mono<Event> event(long token) {
        return eventProvider.event(token);
    }

    @Override
    public Mono<Void> close() {
        return eventProvider.close();
    }
}
