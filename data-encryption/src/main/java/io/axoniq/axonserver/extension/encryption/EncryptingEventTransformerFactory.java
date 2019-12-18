package io.axoniq.axonserver.extension.encryption;

import io.axoniq.axonserver.enterprise.storage.file.MultiContextEventTransformerFactory;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;
import io.axoniq.axonserver.localstorage.transformation.NoOpEventTransformer;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Implementation of {@link MultiContextEventTransformerFactory} that allows for encryption of data at rest.
 * Creates an {@link EventTransformerFactory} instance per context, where each {@link EventTransformerFactory} creates
 * an {@link EncryptingEventTransformer} if the flags for the file are set to non-zero.
 *
 * @author Marc Gathier
 * @since 4.3
 */
@Component
public class EncryptingEventTransformerFactory implements MultiContextEventTransformerFactory {

    private final Function<String, String> keyProvider;
    private final Map<String, EventTransformerFactory> eventTransformerFactoryPerContext = new ConcurrentHashMap<>();

    /**
     * Constructor for the {@link EncryptingEventTransformerFactory}.
     *
     * @param keyProvider function that provides a specific encryption key for a context
     */
    public EncryptingEventTransformerFactory(
            KeyProvider keyProvider) {
        this.keyProvider = keyProvider;
    }

    /**
     * Returns a specific {@link EventTransformerFactory} for a context. The transformer created by the
     * {@link EventTransformerFactory} creates an {@link io.axoniq.axonserver.localstorage.transformation.EventTransformer}
     * based on the file version and flags passed. This information is stored in each file (first 5 bytes).
     * @param context the context name
     * @return the transformer factory
     */
    @Override
    public EventTransformerFactory factoryForContext(String context) {
        return eventTransformerFactoryPerContext.computeIfAbsent(context, c -> (version, flags) -> {
            if (flags == 0) {
                return NoOpEventTransformer.INSTANCE;
            }
            return new EncryptingEventTransformer(keyProvider.apply(context));
        });
    }
}
