package io.axoniq.axonserver.enterprise.cluster.snapshot;

import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import reactor.core.publisher.Flux;

/**
 * @author Milan Savic
 */
public interface SnapshotDataProvider {

    Flux<SerializedObject> provide(long from, long to);

    default int order() {
        return 0;
    }
}
