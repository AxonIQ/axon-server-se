package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.grpc.internal.TransactionWithToken;
import org.springframework.boot.actuate.health.Health;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Author: marc
 */
public class EventStreamReader {
    private final EventStore datafileManagerChain;
    private final EventWriteStorage eventWriteStorage;

    public EventStreamReader(EventStore datafileManagerChain,
                             EventWriteStorage eventWriteStorage) {
        this.datafileManagerChain = datafileManagerChain;
        this.eventWriteStorage = eventWriteStorage;
    }

    public EventStreamController createController(Consumer<EventWithToken> eventWithTokenConsumer, Consumer<Throwable> errorCallback) {
        return new EventStreamController(eventWithTokenConsumer, errorCallback, datafileManagerChain, eventWriteStorage);
    }

    public CompletableFuture<Void>  streamTransactions(long firstToken, Predicate<TransactionWithToken> transactionConsumer) {
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
