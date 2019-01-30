package io.axoniq.axonserver.access.application;

/**
 * Author: marc
 */
public class ApplicationNotFoundException extends RuntimeException {
    public ApplicationNotFoundException(String name) {
        super(name);
    }
}
