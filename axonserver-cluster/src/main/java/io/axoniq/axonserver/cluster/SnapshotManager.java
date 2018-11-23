package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.SerializedObject;

import java.util.List;

/**
 * @author Milan Savic
 */
public interface SnapshotManager {

//    Flux<SerializedObject> captureSnapshotEntries();

    void applySnapshotData(List<SerializedObject> serializedObjects);
}
