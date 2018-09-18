package io.axoniq.axonserver.localstorage;

import io.axoniq.axondb.Event;

import java.util.Optional;
import java.util.function.Consumer;

/**
 * Author: marc
 */
public class AggregateReader {
    private final EventStore datafileManagerChain;
    private final SnapshotReader snapshotReader;

    public AggregateReader(EventStore datafileManagerChain, SnapshotReader snapshotReader) {
        this.datafileManagerChain = datafileManagerChain;
        this.snapshotReader = snapshotReader;
    }

    public void readEvents(String aggregateId, boolean useSnapshots, long minSequenceNumber, Consumer<Event> eventConsumer) {
        long actualMinSequenceNumber = minSequenceNumber;
        if( useSnapshots) {
            Optional<Event> snapshot = snapshotReader.readSnapshot(aggregateId, minSequenceNumber);
            if( snapshot.isPresent()) {
                eventConsumer.accept(snapshot.get());
                actualMinSequenceNumber = snapshot.get().getAggregateSequenceNumber() + 1;
            }
        }
        datafileManagerChain.streamByAggregateId(aggregateId, actualMinSequenceNumber, eventConsumer);

    }

    public long readHighestSequenceNr(String aggregateId) {
        return datafileManagerChain.getLastSequenceNumber(aggregateId).orElse(-1L);
    }
}
