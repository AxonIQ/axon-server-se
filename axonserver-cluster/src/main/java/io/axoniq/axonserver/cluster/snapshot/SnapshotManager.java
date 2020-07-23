package io.axoniq.axonserver.cluster.snapshot;

import io.axoniq.axonserver.grpc.cluster.Role;
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
     * @return a flux of serialized snapshot data
     */
    Flux<SerializedObject> streamSnapshotData(SnapshotContext installationContext);

    /**
     * Streams snapshot data for types that are append only (events/snapshots).
     *
     * @param installationContext information (boundaries for data streams) related to the current snapshot installation
     * @return a flux of serialized snapshot data
     */
    Flux<SerializedObject> streamAppendOnlyData(SnapshotContext installationContext);

    /**
     * Applies a list of serialized snapshot data.
     *
     * @param serializedObjects a list of serialized snapshot data
     * @return a mono indicating that applying is done
     */
    default Mono<Void> applySnapshotData(List<SerializedObject> serializedObjects, Role peerRole) {
        return Mono.when(serializedObjects.stream()
                                          .map(serializedObject -> applySnapshotData(serializedObject, peerRole))
                                          .collect(Collectors.toList()));
    }

    /**
     * Applies a single snapshot data item.
     *
     * @param serializedObject a single piece of serialized snapshot data
     * @return a mono indicating that applying is done
     */
    Mono<Void> applySnapshotData(SerializedObject serializedObject, Role peerRole);

    /**
     * Clears the snapshot data stores. Usually invoked before {@link #applySnapshotData(SerializedObject, Role)} or
     * {@link
     * #applySnapshotData(List, Role)} in order to prepare the stores for applying.
     */
    void clear();
}
