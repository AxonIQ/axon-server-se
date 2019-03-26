package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.grpc.event.EventWithToken;
import org.springframework.boot.actuate.health.Health;

import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * @author Marc Gathier
 */
public class EventStreamReader {
    private final EventStore datafileManagerChain;
    private final EventWriteStorage eventWriteStorage;
    private final EventStreamExecutor eventStreamExecutor;

    public EventStreamReader(EventStore datafileManagerChain,
                             EventWriteStorage eventWriteStorage,
                             EventStreamExecutor eventStreamExecutor) {
        this.datafileManagerChain = datafileManagerChain;
        this.eventWriteStorage = eventWriteStorage;
        this.eventStreamExecutor = eventStreamExecutor;
    }

    public EventStreamController createController(Consumer<SerializedEventWithToken> eventWithTokenConsumer, Consumer<Throwable> errorCallback) {
        return new EventStreamController(eventWithTokenConsumer, errorCallback, datafileManagerChain, eventWriteStorage, eventStreamExecutor);
    }

    public Iterator<SerializedTransactionWithToken> transactionIterator(long firstToken, long limitToken) {
        return datafileManagerChain.transactionIterator(firstToken, limitToken);
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
