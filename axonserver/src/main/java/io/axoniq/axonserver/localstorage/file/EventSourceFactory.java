package io.axoniq.axonserver.localstorage.file;

import java.util.Optional;

/**
 * Factory for the {@link EventSource} instance.
 *
 * @author Milan Savic
 * @author Sara Pellegrini
 */
@FunctionalInterface
public interface EventSourceFactory {

    /**
     * Returns a new instance of {@link EventSource}.
     *
     * @return a new instance of {@link EventSource}.
     */
    Optional<EventSource> create();
}
