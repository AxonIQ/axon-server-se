package io.axoniq.axonserver.extension.encryption;


import org.springframework.stereotype.Component;

/**
 * Simple implementation for {@link KeyProvider} that returns the name of the context as key.
 *
 * @author Marc Gathier
 * @since 4.3
 */
@Component
public class DefaultKeyProvider implements KeyProvider {

    /**
     * Returns the key for a context. This implementation just returns the input.
     *
     * @param context the name of the context
     * @return the key for that context
     */
    @Override
    public String apply(String context) {
        return context;
    }
}
