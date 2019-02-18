package io.axoniq.axonserver.enterprise.cluster.snapshot;

import io.axoniq.axonserver.cluster.snapshot.SnapshotDeserializationException;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import reactor.core.publisher.Flux;

/**
 * Defines a contract for Axon Server snapshot stores. It supports streaming of snapshot data, and applying them.
 *
 * @author Milan Savic
 * @since 4.1
 */
public interface SnapshotDataStore {

    /**
     * Defines an order in which this store should be used when streaming/applying snapshot data. Smaller values come
     * before larger.
     *
     * @return snapshot data store order
     */
    default int order() {
        return 0;
    }

    /**
     * Streams snapshot data within given event sequence boundaries.
     *
     * @param installationContext provides the information needed to define the boundaries of the stream
     * @return a flux of serialized snapshot data
     */
    Flux<SerializedObject> streamSnapshotData(SnapshotInstallationContext installationContext);

    /**
     * Whether this store can apply snapshot data of given {@code type}.
     *
     * @param type the type of snapshot data
     * @return {@code true} if this store can apply snapshot data of given {@code type}, {@code false} otherwise
     */
    boolean canApplySnapshotData(String type);

    /**
     * Applies the snapshot data.
     *
     * @param serializedObject the snapshot data
     * @throws SnapshotDeserializationException is thrown if the store cannot serialize the snapshot data
     */
    void applySnapshotData(SerializedObject serializedObject) throws SnapshotDeserializationException;

    /**
     * Clears relevant data from this store.
     */
    void clear();
}
