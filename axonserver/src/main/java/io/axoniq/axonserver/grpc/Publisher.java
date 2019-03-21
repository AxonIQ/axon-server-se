package io.axoniq.axonserver.grpc;

/**
 * Allows publishing of messages.
 * @author Sara Pellegrini
 */
public interface Publisher<T> {

    /**
     * Publish a message
     * @param message the message
     */
    void publish(T message);

}
