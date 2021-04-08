package io.axoniq.axonserver.refactoring.store.api;

import java.util.Collection;

/**
 * @author Sara Pellegrini
 * @since
 */
public interface EventsQuery {

    long context();

    long initialToken();

    Collection initialBlackList();
}
