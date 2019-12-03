package io.axoniq.axonserver.access.application;

import java.util.function.Supplier;

/**
 * Interface that provides the system token.
 *
 * @author Marc Gathier
 * @since 4.3
 */
public interface SystemTokenProvider extends Supplier<String> {

}
