package io.axoniq.axonserver.commandprocessing.spi;

import reactor.core.publisher.Mono;

/**
 * A general purpose registration. Provides a mean to cancel a registration from a component.
 *
 * @author Sara Pellegrini
 * @author Milan Savic
 */
@FunctionalInterface
public interface Registration {

    /**
     * Cancels the registration.
     *
     * @return a {@link Mono} which will complete when cancellation is done.
     */
    Mono<Void> cancel();
}
