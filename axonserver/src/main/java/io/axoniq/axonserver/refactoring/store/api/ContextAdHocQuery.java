package io.axoniq.axonserver.refactoring.store.api;

/**
 * @author Sara Pellegrini
 * @since
 */
public interface ContextAdHocQuery {

    String context();

    String query();

    boolean liveEvents();
}
