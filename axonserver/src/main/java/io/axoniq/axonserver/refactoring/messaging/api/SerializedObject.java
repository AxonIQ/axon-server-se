package io.axoniq.axonserver.refactoring.messaging.api;

/**
 * @author Sara Pellegrini
 * @since
 */
public interface SerializedObject {

    String type();

    String revision();

    byte[] data();
}
