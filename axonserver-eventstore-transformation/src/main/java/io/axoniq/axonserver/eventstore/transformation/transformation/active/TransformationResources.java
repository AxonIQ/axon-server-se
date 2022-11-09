package io.axoniq.axonserver.eventstore.transformation.transformation.active;

import io.axoniq.axonserver.grpc.event.Event;
import reactor.core.publisher.Mono;

public interface TransformationResources {

    Mono<Event> event(long token);

    Mono<Void> close();
}
