package io.axoniq.axonserver.eventstore.transformation.compact;

public interface EventStoreCompactionTask {

    void start();

    void stop();
}