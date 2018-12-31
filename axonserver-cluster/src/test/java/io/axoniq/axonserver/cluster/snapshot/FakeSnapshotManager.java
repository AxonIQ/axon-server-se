package io.axoniq.axonserver.cluster.snapshot;

import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Sara Pellegrini
 */
public class FakeSnapshotManager implements SnapshotManager{

    @Override
    public Flux<SerializedObject> streamSnapshotChunks(long fromEventSequence, long toEventSequence) {
        return Flux.empty();
    }

    @Override
    public Mono<Void> applySnapshotData(SerializedObject serializedObject) {
        return Mono.empty();
    }

    @Override
    public void clear() {
        // no nop
    }
}