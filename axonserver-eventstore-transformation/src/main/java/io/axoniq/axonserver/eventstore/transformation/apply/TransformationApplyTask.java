package io.axoniq.axonserver.eventstore.transformation.apply;

public interface TransformationApplyTask {

    void start();

    void stop();
}