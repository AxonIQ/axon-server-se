package io.axoniq.axonserver.eventstore.transformation.cancel;

public interface TransformationCancelTask {

    void start();

    void stop();
}
