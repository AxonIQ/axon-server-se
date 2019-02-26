package io.axoniq.axonserver.access.application;

/**
 * @author Marc Gathier
 */
public class ApplicationNotFoundException extends RuntimeException {
    public ApplicationNotFoundException(String name) {
        super(name);
    }
}
