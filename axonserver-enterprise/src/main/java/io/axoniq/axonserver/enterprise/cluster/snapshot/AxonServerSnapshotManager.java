package io.axoniq.axonserver.enterprise.cluster.snapshot;

import io.axoniq.axonserver.cluster.snapshot.SnapshotManager;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Axon Server implementation of {@link SnapshotManager}.
 *
 * @author Milan Savic
 * @since 4.1
 */
public class AxonServerSnapshotManager implements SnapshotManager {

    private final List<SnapshotDataStore> snapshotDataStores;

    /**
     * Creates Axon Server Snapshot Manager with given list of {@code snapshotDataStores}.
     *
     * @param snapshotDataStores snapshot data stores for streaming and applying snapshot data
     */
    public AxonServerSnapshotManager(List<SnapshotDataStore> snapshotDataStores) {
        this.snapshotDataStores = new ArrayList<>(snapshotDataStores);
        this.snapshotDataStores.sort(Comparator.comparingInt(SnapshotDataStore::order));
    }

    @Override
    public Flux<SerializedObject> streamSnapshotData(long fromEventSequence, long toEventSequence) {
        Flux<SerializedObject> stream = Flux.empty();
        for (SnapshotDataStore snapshotDataProvider : snapshotDataStores) {
            stream = stream.concatWith(snapshotDataProvider.streamSnapshotData(fromEventSequence, toEventSequence));
        }
        return stream;
    }

    @Override
    public Mono<Void> applySnapshotData(SerializedObject serializedObject) {
        return Mono.fromRunnable(
                () -> snapshotDataStores
                        .stream()
                        .filter(snapshotDataConsumer -> snapshotDataConsumer
                                .canApplySnapshotData(serializedObject.getType()))
                        .forEach(snapshotDataConsumer -> snapshotDataConsumer.applySnapshotData(serializedObject)));
    }

    @Override
    public void clear() {
        snapshotDataStores.forEach(SnapshotDataStore::clear);
    }
}
