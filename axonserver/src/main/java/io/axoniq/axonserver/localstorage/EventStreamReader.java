package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.grpc.internal.TransactionWithToken;
import org.springframework.boot.actuate.health.Health;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Author: marc
 */
public class EventStreamReader {
    private final EventStore datafileManagerChain;
    private final EventWriteStorage eventWriteStorage;
    private static final Executor threadPool = Executors.newCachedThreadPool();


    public EventStreamReader(EventStore datafileManagerChain,
                             EventWriteStorage eventWriteStorage) {
        this.datafileManagerChain = datafileManagerChain;
        this.eventWriteStorage = eventWriteStorage;
    }

    public EventStreamController createController(Consumer<EventWithToken> eventWithTokenConsumer, Consumer<Throwable> errorCallback) {
        return new EventStreamController(eventWithTokenConsumer, errorCallback, datafileManagerChain, eventWriteStorage);
    }

    public void streamTransactions( long firstToken, StorageCallback storageCallback, Predicate<TransactionWithToken> transactionConsumer) {
        threadPool.execute(() -> datafileManagerChain.streamTransactions(firstToken, storageCallback, transactionConsumer));

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
