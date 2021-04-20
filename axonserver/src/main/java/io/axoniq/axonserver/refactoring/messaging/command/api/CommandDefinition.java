package io.axoniq.axonserver.refactoring.messaging.command.api;

import io.axoniq.axonserver.refactoring.messaging.api.ContextAware;

/**
 * @author Sara Pellegrini
 * @since
 */
public interface CommandDefinition extends ContextAware {

    String name();

    // TODO: 4/20/2021 add JavaDoc
    @Override
    boolean equals(Object other);

    // TODO: 4/20/2021 add JavaDoc
    @Override
    int hashCode();
}
