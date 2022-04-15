package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

public interface TransformationApplyTask {

    void start();

    void stop();
}