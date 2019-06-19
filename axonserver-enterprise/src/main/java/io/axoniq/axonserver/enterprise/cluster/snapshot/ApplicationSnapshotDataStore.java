package io.axoniq.axonserver.enterprise.cluster.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.access.application.ApplicationController;
import io.axoniq.axonserver.access.application.JpaApplication;
import io.axoniq.axonserver.cluster.snapshot.SnapshotDeserializationException;
import io.axoniq.axonserver.cluster.snapshot.SnapshotContext;
import io.axoniq.axonserver.grpc.ApplicationProtoConverter;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import reactor.core.publisher.Flux;

import java.util.List;

import static io.axoniq.axonserver.RaftAdminGroup.isAdmin;
import static io.axoniq.axonserver.grpc.ApplicationProtoConverter.createJpaApplication;


/**
 * Snapshot data store for {@link JpaApplication} data. This data is only distributed in the _admin context.
 *
 * @author Milan Savic
 * @since 4.1
 */
public class ApplicationSnapshotDataStore implements SnapshotDataStore {

    private static final String ENTRY_TYPE = JpaApplication.class.getName();
    private final ApplicationController applicationController;
    private final boolean adminContext;

    /**
     * Creates JpaApplication Snapshot Data Store for streaming/applying {@link JpaApplication} data.
     *
     * @param context               the application context
     * @param applicationController the application controller used for retrieving/saving applications
     */
    public ApplicationSnapshotDataStore(String context, ApplicationController applicationController) {
        this.applicationController = applicationController;
        this.adminContext = isAdmin(context);
    }

    @Override
    public int order() {
        return 0;
    }

    @Override
    public Flux<SerializedObject> streamSnapshotData(SnapshotContext installationContext) {
        if(! adminContext) return Flux.empty();
        List<JpaApplication> applications = applicationController.getApplications();

        return Flux.fromIterable(applications)
                   .map(ApplicationProtoConverter::createApplication)
                   .map(this::toSerializedObject);
    }

    @Override
    public boolean canApplySnapshotData(String type) {
        return adminContext && ENTRY_TYPE.equals(type);
    }


    @Override
    public void applySnapshotData(SerializedObject serializedObject) {
        try {
            io.axoniq.axonserver.grpc.internal.Application applicationMessage = io.axoniq.axonserver.grpc.internal.Application
                    .parseFrom(serializedObject.getData());
            JpaApplication applicationEntity = createJpaApplication(applicationMessage);
            applicationController.synchronize(applicationEntity);
        } catch (InvalidProtocolBufferException e) {
            throw new SnapshotDeserializationException("Unable to deserialize application data.", e);
        }
    }

    @Override
    public void clear() {
        if( adminContext) {
            applicationController.clearApplications();
        }
    }

    private SerializedObject toSerializedObject(io.axoniq.axonserver.grpc.internal.Application application) {
        return SerializedObject.newBuilder()
                               .setType(ENTRY_TYPE)
                               .setData(application.toByteString())
                               .build();
    }
}
