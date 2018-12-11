package io.axoniq.axonserver.cluster.snapshot;

import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Milan Savic
 */
public interface SnapshotManager {

    Flux<SerializedObject> streamSnapshotChunks(long fromEventSequence, long toEventSequence);

    default Mono<Void> applySnapshotData(List<SerializedObject> serializedObjects) {
        return Mono.when(serializedObjects.stream()
                                          .map(this::applySnapshotData)
                                          .collect(Collectors.toList()));
    }

    Mono<Void> applySnapshotData(SerializedObject serializedObject);
}
