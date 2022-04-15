package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.axoniq.axonserver.eventstore.transformation.requestprocessor.EventStoreTransformationJpa.Status.APPLYING;
import static io.axoniq.axonserver.eventstore.transformation.requestprocessor.EventStoreTransformationJpa.Status.ROLLING_BACK;

public interface TransformationRollBackTask {

    void start();

    void stop();
}