package io.axoniq.axonserver.util;

/**
 * @author Marc Gathier
 */
public class ContextNotFoundException extends RuntimeException {

    public ContextNotFoundException(String message) {
        super(message);
    }
}
