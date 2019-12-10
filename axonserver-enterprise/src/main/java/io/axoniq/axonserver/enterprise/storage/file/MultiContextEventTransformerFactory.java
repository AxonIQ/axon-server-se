package io.axoniq.axonserver.enterprise.storage.file;

import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;

/**
 * Factory to create a context specific EventTransformerFactory.
 * Each event transformer factory can create an event transformer to use for a specific file (based on file version
 * number and flags).
 *
 * @author Marc Gathier
 * @since 4.3
 */
public interface MultiContextEventTransformerFactory {

    /**
     * Returns an {@link EventTransformerFactory} for the given {@code context}.
     *
     * @param context the context name
     * @return the transformer factory
     */
    EventTransformerFactory factoryForContext(String context);
}
