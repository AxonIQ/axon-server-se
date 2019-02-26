package io.axoniq.axonserver.localstorage;

/**
 * @author Marc Gathier
 */
@FunctionalInterface
public interface Registration {
    void cancel();
}
