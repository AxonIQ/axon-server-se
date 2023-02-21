package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import io.axoniq.axonserver.grpc.event.Event;
import reactor.core.publisher.Mono;

@FunctionalInterface
public interface EventProvider {

    Mono<Event> event(long token);
}
