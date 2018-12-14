package io.axoniq.axonserver.enterprise.cluster.snapshot;

import io.axoniq.axonserver.cluster.snapshot.SnapshotManager;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Comparator;
import java.util.List;

/**
 * @author Milan Savic
 */
public class AxonServerSnapshotManager implements SnapshotManager {

    private final List<SnapshotDataProvider> snapshotDataProviders;

    public AxonServerSnapshotManager(List<SnapshotDataProvider> snapshotDataProviders) {
        this.snapshotDataProviders = snapshotDataProviders;
    }

    @Override
    public Flux<SerializedObject> streamSnapshotChunks(long fromEventSequence, long toEventSequence) {
        snapshotDataProviders.sort(Comparator.comparingInt(SnapshotDataProvider::order));

        Flux<SerializedObject> stream = Flux.empty();
        for (SnapshotDataProvider snapshotDataProvider : snapshotDataProviders) {
            stream = stream.concatWith(snapshotDataProvider.provide(fromEventSequence, toEventSequence));
        }
        return stream;
    }

    @Override
    public Mono<Void> applySnapshotData(SerializedObject serializedObject) {
        return Mono.fromRunnable(
                () -> snapshotDataProviders
                        .stream()
                        .filter(snapshotDataConsumer -> snapshotDataConsumer.canConsume(serializedObject.getType()))
                        .forEach(snapshotDataConsumer -> snapshotDataConsumer.consume(serializedObject)));
    }
}
