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

    public Optional<SerializedEvent> readSnapshot(String aggregateId, long minSequenceNumber) {
            return datafileManagerChain
                    .getLastEvent(aggregateId, minSequenceNumber)
                    .map(s -> new SerializedEvent(Event.newBuilder(s.asEvent()).setSnapshot(true).build()));
    }

    public void streamByAggregateId(String aggregateId, long minSequenceNumber, long maxSequenceNumber, int maxResults, Consumer<SerializedEvent> eventConsumer) {
        datafileManagerChain.streamByAggregateId(aggregateId, minSequenceNumber, maxSequenceNumber, maxResults, eventConsumer);
    }
}
