package io.axoniq.axonserver.refactoring.store.api;

/**
 * @author Sara Pellegrini
 * @since
 */
public interface AggregateSnapshotsQuery {

    String context();

    String aggregateId();

    long initialSequenceNumber();

    long maxSequenceNumber();

    int maxResults();
}
