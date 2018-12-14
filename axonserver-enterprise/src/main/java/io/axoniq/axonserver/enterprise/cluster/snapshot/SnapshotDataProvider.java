package io.axoniq.axonserver.enterprise.cluster.snapshot;

import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import reactor.core.publisher.Flux;

/**
 * @author Milan Savic
 */
public interface SnapshotDataProvider {

    default int order() {
        return 0;
    }

    Flux<SerializedObject> provide(long from, long to);

    boolean canConsume(String type);

    void consume(SerializedObject serializedObject);
}
