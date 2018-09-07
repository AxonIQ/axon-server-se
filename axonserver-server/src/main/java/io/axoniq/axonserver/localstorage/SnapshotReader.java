package io.axoniq.axonserver.localstorage;

import io.axoniq.axondb.Event;

import java.util.Optional;

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

}
