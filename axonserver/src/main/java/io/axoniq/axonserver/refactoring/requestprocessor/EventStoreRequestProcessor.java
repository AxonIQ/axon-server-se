package io.axoniq.axonserver.refactoring.requestprocessor;

import io.axoniq.axonserver.refactoring.api.Authentication;
import io.axoniq.axonserver.refactoring.requestprocessor.store.EventStoreService;
import io.axoniq.axonserver.refactoring.store.api.*;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;

@Service
public class EventStoreRequestProcessor implements EventStoreService {

    private final EventStore eventStore;

    public EventStoreRequestProcessor(EventStore eventStore) {
        this.eventStore = eventStore;
    }

    @Override
    public Mono<Void> appendEvents(String context, Flux<Event> events, Authentication authentication) {
        // TODO: 5/11/21 logging
        // TODO: 5/11/21 validate authentication
        return eventStore.appendEvents(context, events, authentication);
    }

    @Override
    public Mono<Void> appendSnapshot(String context, Snapshot snapshot, Authentication authentication) {
        // TODO: 5/11/21 logging
        // TODO: 5/11/21 validate authentication
        return eventStore.appendSnapshot(context, snapshot, authentication);
    }

    @Override
    public Flux<Event> aggregateEvents(AggregateEventsQuery aggregateEventsQuery, Authentication authentication) {
        // TODO: 5/11/21 logging
        // TODO: 5/11/21 validate authentication
        return eventStore.aggregateEvents(aggregateEventsQuery, authentication);
    }

    @Override
    public Flux<EventWithToken> events(EventsQuery eventsQuery, Flux<PayloadType> blackListUpdates, Authentication authentication) {
        // TODO: 5/11/21 logging
        // TODO: 5/11/21 validate authentication
        return eventStore.events(eventsQuery, blackListUpdates, authentication);
    }

    @Override
    public Mono<Long> lastEventToken(String context, Authentication authentication) {
        // TODO: 5/11/21 logging
        // TODO: 5/11/21 validate authentication
        return eventStore.lastEventToken(context);
    }

    @Override
    public Mono<Long> firstEventToken(String context, Authentication authentication) {
        // TODO: 5/11/21 logging
        // TODO: 5/11/21 validate authentication
        return eventStore.firstEventToken(context);
    }

    @Override
    public Mono<Long> eventTokenAt(String context, Instant instant, Authentication authentication) {
        // TODO: 5/11/21 logging
        // TODO: 5/11/21 validate authentication
        return eventStore.eventTokenAt(context, instant);
    }

    @Override
    public Mono<Long> highestSequenceNumber(String context, String aggregateId, Authentication authentication) {
        // TODO: 5/11/21 logging
        // TODO: 5/11/21 validate authentication
        return eventStore.highestSequenceNumber(context, aggregateId);
    }

    @Override
    public Flux<EventQueryResponse> queryEvents(AdHocEventsQuery query, Authentication authentication) {
        // TODO: 5/11/21 logging
        // TODO: 5/11/21 validate authentication
        return eventStore.queryEvents(query, authentication);
    }

    @Override
    public Flux<SnapshotQueryResponse> querySnapshots(AdHocSnapshotsQuery query, Authentication authentication) {
        // TODO: 5/11/21 logging
        // TODO: 5/11/21 validate authentication
        return eventStore.querySnapshots(query, authentication);
    }

    @Override
    public Flux<Snapshot> aggregateSnapshots(AggregateSnapshotsQuery query, Authentication authentication) {
        // TODO: 5/11/21 logging
        // TODO: 5/11/21 validate authentication
        return eventStore.aggregateSnapshots(query, authentication);
    }
}
