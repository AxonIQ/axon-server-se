package io.axoniq.axonserver.cluster.snapshot;

import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * @author Milan Savic
 */
public interface SnapshotManager {

    Flux<SerializedObject> streamSnapshotChunks(long fromEventSequence, long toEventSequence);

    Mono<Void> applySnapshotData(List<SerializedObject> serializedObjects);
}
