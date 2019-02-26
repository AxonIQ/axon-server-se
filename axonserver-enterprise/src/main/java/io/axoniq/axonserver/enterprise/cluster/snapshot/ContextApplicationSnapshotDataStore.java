package io.axoniq.axonserver.enterprise.cluster.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.access.application.JpaContextApplication;
import io.axoniq.axonserver.access.application.JpaContextApplicationController;
import io.axoniq.axonserver.cluster.snapshot.SnapshotContext;
import io.axoniq.axonserver.cluster.snapshot.SnapshotDeserializationException;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import io.axoniq.axonserver.grpc.internal.ContextApplication;
import reactor.core.publisher.Flux;

import java.util.List;


/**
 * Snapshot data store for {@link JpaContextApplication} data. This data is distributed for all contexts.
 *
 * @author Marc Gathier
 * @since 4.1
 */
public class ContextApplicationSnapshotDataStore implements SnapshotDataStore {
    private static final String TYPE = JpaContextApplication.class.getName();
    private final String context;
    private final JpaContextApplicationController applicationController;

    /**
     * Creates JpaApplication Snapshot Data Store for streaming/applying {@link JpaContextApplication} data.
     *
     * @param context               the application context
     * @param applicationController the application controller used for retrieving/saving applications
     */
    public ContextApplicationSnapshotDataStore(String context, JpaContextApplicationController applicationController) {
        this.context = context;
        this.applicationController = applicationController;
    }

    @Override
    public int order() {
        return 0;
    }

    @Override
    public Flux<SerializedObject> streamSnapshotData(SnapshotContext snapshotContext) {
        List<ContextApplication> applications = applicationController.getApplicationsForContext(context);

        return Flux.fromIterable(applications)
                   .map(this::toSerializedObject);
    }

    @Override
    public boolean canApplySnapshotData(String type) {
        return TYPE.equals(type);
    }


    @Override
    public void applySnapshotData(SerializedObject serializedObject) {
        try {
            ContextApplication applicationMessage = ContextApplication.parseFrom(serializedObject.getData());
            applicationController.mergeApplication(applicationMessage);
        } catch (InvalidProtocolBufferException e) {
            throw new SnapshotDeserializationException("Unable to deserialize application data.", e);
        }
    }

    @Override
    public void clear() {
        applicationController.deleteByContext(context);
    }

    private SerializedObject toSerializedObject(ContextApplication application) {
        return SerializedObject.newBuilder()
                               .setType(TYPE)
                               .setData(application.toByteString())
                               .build();
    }
}
