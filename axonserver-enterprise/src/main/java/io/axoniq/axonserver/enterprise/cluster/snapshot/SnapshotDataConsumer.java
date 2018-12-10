package io.axoniq.axonserver.enterprise.cluster.snapshot;

import io.axoniq.axonserver.grpc.cluster.SerializedObject;

/**
 * @author Milan Savic
 */
public interface SnapshotDataConsumer {

    boolean canConsume(String type);

    void consume(SerializedObject serializedObject);
}
