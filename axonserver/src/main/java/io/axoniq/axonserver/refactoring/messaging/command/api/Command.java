package io.axoniq.axonserver.refactoring.messaging.command.api;

import io.axoniq.axonserver.refactoring.messaging.api.Client;
import io.axoniq.axonserver.refactoring.messaging.api.ContextAware;
import io.axoniq.axonserver.refactoring.messaging.api.Message;

import java.time.Instant;

/**
 * @author Sara Pellegrini
 * @since
 */
public interface Command extends ContextAware {

    @Override
    default String context() {
        return definition().context();
    }

    CommandDefinition definition();

    Message message();

    String routingKey();

    Instant timestamp();

    default Client requester() {
        return new Client() {
            @Override
            public String id() {
                return "anonymous";
            }

            @Override
            public String applicationName() {
                return "anonymous";
            }
        };
    }
}
