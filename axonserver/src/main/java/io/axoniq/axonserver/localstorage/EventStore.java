package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.localstorage.transaction.PreparedTransaction;
import org.springframework.boot.actuate.health.Health;
import org.springframework.data.util.CloseableIterator;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * @author Marc Gathier
 */
public interface EventStore {

    void init(boolean validate);

    default CompletableFuture<Long> store(PreparedTransaction eventList) {
        CompletableFuture<Long> completableFuture = new CompletableFuture<>();
        completableFuture.completeExceptionally(new UnsupportedOperationException("Cannot create writable datafile"));
        return completableFuture;
    }

    default long getLastToken() {
        return -1;
    }

    Optional<Long> getLastSequenceNumber(String aggregateIdentifier);

    default void cleanup() {
    }

    boolean streamEvents(long token, Predicate<SerializedEventWithToken> onEvent);

    Optional<SerializedEvent> getLastEvent(String aggregateId, long minSequenceNumber);

    default void reserveSequenceNumbers(List<SerializedEvent> events) {
    }

    void streamByAggregateId(String aggregateId, long actualMinSequenceNumber, Consumer<SerializedEvent> eventConsumer);

    void streamByAggregateId(String aggregateId, long actualMinSequenceNumber, long actualMaxSequenceNumber, int maxResults, Consumer<SerializedEvent> eventConsumer);

    PreparedTransaction prepareTransaction( List<SerializedEvent> eventList);

    default boolean replicated() {
        return false;
    }

    EventTypeContext getType();

    Iterator<SerializedTransactionWithToken> transactionIterator(long firstToken);

    Iterator<SerializedTransactionWithToken> transactionIterator(long firstToken, long limitToken);

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

    default void stepDown() {

    }

    default boolean contains( SerializedTransactionWithToken newTransaction) {
        return true;
    }

    /**
     * Return a closeable iterator to iterate over all events starting at token start.
     * @param start
     * @return closeable iterator of SerializedEventWithToken
     */
    CloseableIterator<SerializedEventWithToken> getGlobalIterator(long start);

    default byte transactionVersion() {
        return 0;
    }

    /**
     * Returns the next token that will be used by the event store. Does not change the token.
     * @return the next token
     */
    long nextToken();

    /**
     * Deletes all event data in the Event Store (Only intended for development environments).
     */
    void deleteAllEventData();
}
