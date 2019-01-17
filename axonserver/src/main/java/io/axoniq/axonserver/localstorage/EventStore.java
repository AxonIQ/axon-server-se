package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.localstorage.transaction.PreparedTransaction;
import org.springframework.boot.actuate.health.Health;
import org.springframework.data.util.CloseableIterator;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Author: marc
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

    PreparedTransaction prepareTransaction(List<SerializedEvent> eventList);

    default boolean replicated() {
        return false;
    }

    EventTypeContext getType();

    void streamTransactions(long firstToken,
                            Predicate<SerializedTransactionWithToken> transactionConsumer);

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

    default boolean contains( SerializedTransactionWithToken newTransaction) {
        return true;
    }

    /**
     * Return a closeable iterator to iterate over all events starting at token start.
     * @param start
     * @return closeable iterator of SerializedEventWithToken
     */
    CloseableIterator<SerializedEventWithToken> getGlobalIterator(long start);
}
