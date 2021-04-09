package io.axoniq.axonserver.refactoring.messaging.query.api;

import io.axoniq.axonserver.refactoring.messaging.api.ContextAware;

/**
 * @author Sara Pellegrini
 * @since
 */
public interface QueryDefinition extends ContextAware {

    String queryName();

    String responseType();
}
