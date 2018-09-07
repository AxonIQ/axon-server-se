package io.axoniq.platform.application;

/**
 * Author: marc
 */
public class ApplicationNotFoundException extends RuntimeException {
    public ApplicationNotFoundException(String name) {
        super(name);
    }
}
