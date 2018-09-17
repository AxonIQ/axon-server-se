package io.axoniq.axonserver.localstorage;

import io.axoniq.axondb.Event;
import io.axoniq.axondb.grpc.EventWithToken;
import io.axoniq.axonserver.internal.grpc.TransactionWithToken;
import io.axoniq.axonserver.localstorage.transaction.PreparedTransaction;
import org.springframework.boot.actuate.health.Health;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Author: marc
 */
public interface EventStore {

    void init(boolean validate);

    default void store(PreparedTransaction eventList, StorageCallback storageCallback) {
        storageCallback.onError(new UnsupportedOperationException("Cannot create writable datafile"));
    }

    default long getLastToken() {
        return -1;
    }

    Optional<Long> getLastSequenceNumber(String aggregateIdentifier);

    default void cleanup() {
    }

    void streamEvents(long token, StorageCallback callbacks, Predicate<EventWithToken> onEvent);

    Optional<Event> getLastEvent(String aggregateId, long minSequenceNumber);

    default void reserveSequenceNumbers(List<Event> events) {
    }

    void streamByAggregateId(String aggregateId, long actualMinSequenceNumber, Consumer<Event> eventConsumer);

    PreparedTransaction prepareTransaction(List<Event> eventList);

    default boolean replicated() {
        return false;
    }

    EventTypeContext getType();

    void streamTransactions(long firstToken, StorageCallback callbacks,
                            Predicate<TransactionWithToken> transactionConsumer);

    void query(long minToken, long minTimestamp, Predicate<EventWithToken> consumer);

    default Stream<String> getBackupFilenames(long lastSegmentBackedUp) {
        throw new UnsupportedOperationException();
    }

    long getFirstToken();

    long getTokenAt(long instant);

    default void health(Health.Builder builder) {
    }

    default void rollback(long token) {
    }
}
