package io.axoniq.axonserver.migration;

import java.util.List;

/**
 * Author: marc
 */
public interface EventProducer {
    List<? extends DomainEvent> findEvents(long lastProcessedToken, int batchSize);

    List<? extends SnapshotEvent> findSnapshots(String lastProcessedTimestamp, int batchSize);
}
