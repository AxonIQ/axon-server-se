package io.axoniq.axonserver.eventstore.transformation;

/**
 * @author Sara Pellegrini
 * @since
 */
public interface TransformationTask {

    void start();

    void stop();
}
