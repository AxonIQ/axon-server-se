package io.axoniq.axonserver.commandprocessing.spi;

/**
 * Marker interface for Command Processing interceptors.
 *
 * @author Sara Pellegrini
 * @author Milan Savic
 */
public interface Interceptor {

    default int priority() {
        return 0;
    }
}
