package io.axoniq.axonserver.refactoring.store.api;

/**
 * @author Sara Pellegrini
 * @since
 */
public interface EventWithToken extends Event {

    long token();
}
