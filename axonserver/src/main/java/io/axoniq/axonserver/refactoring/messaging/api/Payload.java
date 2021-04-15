package io.axoniq.axonserver.refactoring.messaging.api;

/**
 * @author Sara Pellegrini
 * @since
 */
public interface Payload {

    String type();

    String revision();

    byte[] data();
}
