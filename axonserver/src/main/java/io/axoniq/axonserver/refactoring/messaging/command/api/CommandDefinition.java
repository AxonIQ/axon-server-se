package io.axoniq.axonserver.refactoring.messaging.command.api;

import io.axoniq.axonserver.refactoring.messaging.api.ContextAware;

/**
 * @author Sara Pellegrini
 * @since
 */
public interface CommandDefinition extends ContextAware {

    String name();
}
