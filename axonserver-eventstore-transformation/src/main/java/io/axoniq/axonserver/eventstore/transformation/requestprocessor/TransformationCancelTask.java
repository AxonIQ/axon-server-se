package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

public interface TransformationCancelTask {

    void start();

    void stop();
}
