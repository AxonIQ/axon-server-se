package io.axoniq.axonserver.refactoring.store;

import io.axoniq.axonserver.refactoring.store.api.Event;
import io.axoniq.axonserver.refactoring.store.api.EventQueryResponse;
import io.axoniq.axonserver.refactoring.store.api.EventWithToken;
import io.axoniq.axonserver.refactoring.store.api.LocalEventStorage;
import io.axoniq.axonserver.refactoring.store.api.PayloadType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;

/**
 * it uses storage engine to persist,
 * takes care of transaction,
 * takes care of exclusive access,
 *
 * @author Sara Pellegrini
 * @since
 */
public class DefaultLocalEventStorage implements LocalEventStorage {


    @Override
    public Mono<Long> highestSequenceNumber(String aggregateId) {
        return null;
    }

    @Override
    public Flux<Event> aggregateEvents(String aggregateId, long firstSequenceNumber, long maxSequenceNumber) {
        return null;
    }

    @Override //invoked by apply log entry
    public Mono<Void> appendEvents(Flux<Event> events) {
        // write event and update indexed, checksums

        // client start transaction (EventStore)
        // follower proxies them to leader (????)
        // leader reserve sequence number & token (????)
        // leader creates log entry & replicates (cluster module)
        // leader applies (LocalEventStorage) and commit
        // followers apply (LocalEventStorage)
        return null;
    }

    @Override
    public Flux<EventWithToken> events(long initialToken, Flux<PayloadType> blackListUpdates) {
        return null;
    }

    @Override
    public Flux<EventQueryResponse> queryEvents(String query) {
        return null;
    }

    @Override
    public Mono<Long> lastToken() {
        return null;
    }

    @Override
    public Mono<Long> firstToken() {
        return null;
    }

    @Override
    public Mono<Long> eventTokenAt(Instant instant) {
        return null;
    }
}
