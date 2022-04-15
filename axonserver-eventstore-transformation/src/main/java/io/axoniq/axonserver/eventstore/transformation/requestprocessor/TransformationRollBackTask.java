package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

public interface TransformationRollBackTask {

    void start();

    void stop();
}

