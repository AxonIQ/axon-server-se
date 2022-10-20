package io.axoniq.axonserver.commandprocessing.spi;

public class CapacityException extends RuntimeException {

    public CapacityException(String message) {
        super(message);
    }
}
