package io.axoniq.axonserver.refactoring.store.api;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;

/**
 * @author Sara Pellegrini
 * @since 4.6
 */
public interface EventStorage {

    Mono<Long> highestSequenceNumber(String aggregateId);

    Flux<Event> aggregateEvents(String aggregateId, long firstSequenceNumber, long maxSequenceNumber);

    Mono<Void> appendEvents(Flux<Event> events);

    Flux<EventWithToken> events(long initialToken, Flux<PayloadType> blackListUpdates);

    Flux<EventQueryResponse> queryEvents(String query);

    Mono<Long> lastToken();

    Mono<Long> firstToken();

    Mono<Long> eventTokenAt(Instant instant);
}
