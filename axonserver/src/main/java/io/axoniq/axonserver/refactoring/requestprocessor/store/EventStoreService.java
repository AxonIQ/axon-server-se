package io.axoniq.axonserver.refactoring.requestprocessor.store;

import io.axoniq.axonserver.refactoring.api.Authentication;
import io.axoniq.axonserver.refactoring.store.api.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;

public interface EventStoreService {

    Mono<Void> appendEvents(String context, Flux<Event> events, Authentication authentication);

    Mono<Void> appendSnapshot(String context, Snapshot snapshot, Authentication authentication);

    Flux<Event> aggregateEvents(AggregateEventsQuery aggregateEventsQuery, Authentication authentication);

    Flux<Snapshot> aggregateSnapshots(AggregateSnapshotsQuery query, Authentication authentication);

    Flux<EventWithToken> events(EventsQuery eventsQuery, Flux<PayloadType> blackListUpdates, Authentication authentication);

    Mono<Long> lastEventToken(String context, Authentication authentication);

    Mono<Long> firstEventToken(String context, Authentication authentication);

    Mono<Long> eventTokenAt(String context, Instant instant, Authentication authentication);

    Mono<Long> highestSequenceNumber(String context, String aggregateId, Authentication authentication);

    Flux<EventQueryResponse> queryEvents(AdHocEventsQuery query, Authentication authentication);

    Flux<SnapshotQueryResponse> querySnapshots(AdHocSnapshotsQuery query, Authentication authentication);
}
