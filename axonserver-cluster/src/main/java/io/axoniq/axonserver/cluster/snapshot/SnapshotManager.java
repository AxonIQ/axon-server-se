package io.axoniq.axonserver.cluster.snapshot;

import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Manages RAFT snapshot process - streaming of snapshot data and applying snapshot data.
 *
 * @author Milan Savic
 * @since 4.1
 */
public interface SnapshotManager {

    /**
     * Streams snapshot data within given event sequence boundaries.
     *
     * @param fromEventSequence lower boundary (inclusive) in terms of event sequence of snapshot data
     * @param toEventSequence   upper boundary (inclusive) in terms of event sequence of snapshot data
     * @return a flux of serialized snapshot data
     */
    Flux<SerializedObject> streamSnapshotData(long fromEventSequence, long toEventSequence);

    /**
     * Applies a list of serialized snapshot data.
     *
     * @param serializedObjects a list of serialized snapshot data
     * @return a mono indicating that applying is done
     */
    default Mono<Void> applySnapshotData(List<SerializedObject> serializedObjects) {
        return Mono.when(serializedObjects.stream()
                                          .map(this::applySnapshotData)
                                          .collect(Collectors.toList()));
    }

    /**
     * Applies a single snapshot data item.
     *
     * @param serializedObject a single piece of serialized snapshot data
     * @return a mono indicating that applying is done
     */
    Mono<Void> applySnapshotData(SerializedObject serializedObject);

    /**
     * Clears the snapshot data stores. Usually invoked before {@link #applySnapshotData(SerializedObject)} or {@link
     * #applySnapshotData(List)} in order to prepare the stores for applying.
     */
    void clear();
}
