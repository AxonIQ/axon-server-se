package io.axoniq.axonserver.localstorage;

import java.util.Optional;
import java.util.function.Consumer;

/**
 * @author Marc Gathier
 */
public class AggregateReader {
    private final EventStorageEngine datafileManagerChain;
    private final SnapshotReader snapshotReader;

    public AggregateReader(EventStorageEngine datafileManagerChain, SnapshotReader snapshotReader) {
        this.datafileManagerChain = datafileManagerChain;
        this.snapshotReader = snapshotReader;
    }

    public void readEvents(String aggregateId, boolean useSnapshots, long minSequenceNumber, Consumer<SerializedEvent> eventConsumer) {
        long actualMinSequenceNumber = minSequenceNumber;
        if( useSnapshots) {
            Optional<SerializedEvent> snapshot = snapshotReader.readSnapshot(aggregateId, minSequenceNumber);
            if( snapshot.isPresent()) {
                eventConsumer.accept(snapshot.get());
                actualMinSequenceNumber = snapshot.get().asEvent().getAggregateSequenceNumber() + 1;
            }
        }
        datafileManagerChain.streamByAggregateId(aggregateId, actualMinSequenceNumber, eventConsumer);

    }
    public void readSnapshots(String aggregateId, long minSequenceNumber, long maxSequenceNumber, int maxResults, Consumer<SerializedEvent> eventConsumer) {
        snapshotReader.streamByAggregateId(aggregateId, minSequenceNumber, maxSequenceNumber,
                                           maxResults > 0 ? maxResults : Integer.MAX_VALUE, eventConsumer);

    }

    public long readHighestSequenceNr(String aggregateId) {
        return datafileManagerChain.getLastSequenceNumber(aggregateId).orElse(-1L);
    }
}
