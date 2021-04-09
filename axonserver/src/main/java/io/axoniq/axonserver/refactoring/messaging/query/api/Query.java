package io.axoniq.axonserver.refactoring.messaging.query.api;

import io.axoniq.axonserver.refactoring.messaging.api.Client;
import io.axoniq.axonserver.refactoring.messaging.api.ContextAware;
import io.axoniq.axonserver.refactoring.messaging.api.Message;

import java.time.Instant;

/**
 * @author Sara Pellegrini
 * @since
 */
public interface Query extends ContextAware {

    @Override
    default String context() {
        return definition().context();
    }

    QueryDefinition definition();

    Message message();

    Instant timestamp();

    Client requester();
}
