package io.axoniq.axonserver.refactoring.store.api;

/**
 * @author Sara Pellegrini
 * @since
 */
public interface AggregateEventsQuery {

    String context();

    String aggregateId();

    boolean isSnapshotAllowed();
}
