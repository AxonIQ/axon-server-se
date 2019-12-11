package io.axoniq.axonserver.enterprise.storage.file;

import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;

/**
 * Default implementation for the {@link MultiContextEventTransformerFactory}, returns the default {@link
 * EventTransformerFactory}
 * for all contexts (default behaviour is no specific transformation per context).
 *
 * @author Marc Gathier
 * @since 4.3
 */
public class DefaultMultiContextEventTransformerFactory implements MultiContextEventTransformerFactory {

    private final EventTransformerFactory eventTransformerFactory;

    /**
     * Constructor.
     *
     * @param eventTransformerFactory the default event transformer factory
     */
    public DefaultMultiContextEventTransformerFactory(
            EventTransformerFactory eventTransformerFactory) {
        this.eventTransformerFactory = eventTransformerFactory;
    }

    /**
     * Returns the {@link EventTransformerFactory} to use for this context.
     *
     * @param context the context name
     * @return the transformer factory.
     */
    @Override
    public EventTransformerFactory factoryForContext(String context) {
        return eventTransformerFactory;
    }
}
