package io.axoniq.axonserver.extension.encryption;

import java.util.function.Function;

/**
 * Defines the interface for a function that returns a key for a specific context.
 *
 * @author Marc Gathier
 * @since 4.3
 */
public interface KeyProvider extends Function<String, String> {

}
