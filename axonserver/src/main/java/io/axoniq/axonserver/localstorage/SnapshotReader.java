package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.grpc.event.Event;

import java.util.Optional;
import java.util.function.Consumer;

/**
 * Author: marc
 */
public class SnapshotReader {
    private final EventStore datafileManagerChain;

    public SnapshotReader(EventStore datafileManagerChain) {
        this.datafileManagerChain = datafileManagerChain;
    }

    public Optional<Event> readSnapshot(String aggregateId, long minSequenceNumber) {
            return datafileManagerChain
                    .getLastEvent(aggregateId, minSequenceNumber)
                    .map(s -> Event.newBuilder(s).setSnapshot(true).build());
    }

    public void streamByAggregateId(String aggregateId, long minSequenceNumber, Consumer<Event> eventConsumer) {
        datafileManagerChain.streamByAggregateId(aggregateId, minSequenceNumber, eventConsumer);
    }
}
