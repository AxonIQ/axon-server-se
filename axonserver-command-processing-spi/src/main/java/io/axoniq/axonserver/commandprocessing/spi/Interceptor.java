package io.axoniq.axonserver.commandprocessing.spi;

/**
 * Marker interface for Command Processing interceptors.
 *
 * @author Sara Pellegrini
 * @author Milan Savic
 */
public interface Interceptor {

    int PRIORITY_FIRST = Integer.MIN_VALUE;
    int PRIORITY_LAST = Integer.MAX_VALUE;
    int PRIORITY_LATER = 100;

    default int priority() {
        return 0;
    }
}
