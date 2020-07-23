package io.axoniq.axonserver.cluster.snapshot;

import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Sara Pellegrini
 */
public class FakeSnapshotManager implements SnapshotManager {

    @Override
    public Flux<SerializedObject> streamSnapshotData(SnapshotContext installationContext) {
        return Flux.empty();
    }

    @Override
    public Flux<SerializedObject> streamAppendOnlyData(SnapshotContext installationContext) {
        return Flux.empty();
    }

    @Override
    public Mono<Void> applySnapshotData(SerializedObject serializedObject, Role role) {
        return Mono.empty();
    }

    @Override
    public void clear() {
        // no nop
    }
}