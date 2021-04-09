package io.axoniq.axonserver.refactoring.messaging.query.api;

import io.axoniq.axonserver.refactoring.messaging.api.Registration;

import java.util.List;

/**
 * @author Sara Pellegrini
 * @since
 */
public interface QueryRegistry {

    Registration register(QueryHandler commandHandler);

    List<QueryHandler> handlers();
}
