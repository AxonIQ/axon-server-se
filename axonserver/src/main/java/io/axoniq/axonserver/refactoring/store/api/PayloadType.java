package io.axoniq.axonserver.refactoring.store.api;

/**
 * @author Sara Pellegrini
 * @since
 */
public interface PayloadType {

    String type();

    String revision();
}
