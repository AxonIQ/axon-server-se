package io.axoniq.axonserver.localstorage.file;

import java.util.Optional;

/**
 * @author Milan Savic
 */
public interface EventSourceFactory {

    Optional<EventSource> create();
}
