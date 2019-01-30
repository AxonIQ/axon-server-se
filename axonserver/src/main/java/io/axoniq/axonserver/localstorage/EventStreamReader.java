package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.grpc.event.EventWithToken;
import org.springframework.boot.actuate.health.Health;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * @author Marc Gathier
 */
public class EventStreamReader {
    private final EventStore datafileManagerChain;
    private final EventWriteStorage eventWriteStorage;

    public EventStreamReader(EventStore datafileManagerChain,
                             EventWriteStorage eventWriteStorage) {
        this.datafileManagerChain = datafileManagerChain;
        this.eventWriteStorage = eventWriteStorage;
    }

    public EventStreamController createController(Consumer<SerializedEventWithToken> eventWithTokenConsumer, Consumer<Throwable> errorCallback) {
        return new EventStreamController(eventWithTokenConsumer, errorCallback, datafileManagerChain, eventWriteStorage);
    }

    public Iterator<SerializedTransactionWithToken> transactionIterator(long firstToken) {
        return datafileManagerChain.transactionIterator(firstToken);
    }

    public Iterator<SerializedTransactionWithToken> transactionIterator(long firstToken, long limitToken) {
        return datafileManagerChain.transactionIterator(firstToken, limitToken);
    }

    @Deprecated
    /**
     * @deprecated use {@link #transactionIterator(long)} instead
     */
    public CompletableFuture<Void>  streamTransactions(long firstToken, Predicate<SerializedTransactionWithToken> transactionConsumer) {
        return CompletableFuture.runAsync(() -> datafileManagerChain.streamTransactions(firstToken, transactionConsumer));
    }

    public void query(long minToken, long minTimestamp, Predicate<EventWithToken> consumer) {
        datafileManagerChain.query( minToken, minTimestamp, consumer);
    }

    public long getFirstToken() {
        return datafileManagerChain.getFirstToken();
    }

    public long getTokenAt(long instant) {
        return datafileManagerChain.getTokenAt(instant);
    }

    public void health(Health.Builder builder) {
        datafileManagerChain.health(builder);
    }
}
