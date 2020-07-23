package io.axoniq.axonserver.enterprise.replication.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.access.application.AdminApplication;
import io.axoniq.axonserver.access.application.AdminApplicationController;
import io.axoniq.axonserver.cluster.snapshot.SnapshotContext;
import io.axoniq.axonserver.cluster.snapshot.SnapshotDeserializationException;
import io.axoniq.axonserver.grpc.ApplicationProtoConverter;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import reactor.core.publisher.Flux;

import java.util.List;

import static io.axoniq.axonserver.RaftAdminGroup.isAdmin;
import static io.axoniq.axonserver.grpc.ApplicationProtoConverter.createJpaApplication;


/**
 * Snapshot data store for {@link AdminApplication} data. This data is only distributed in the _admin context.
 *
 * @author Milan Savic
 * @since 4.1
 */
public class AdminApplicationSnapshotDataStore implements SnapshotDataStore {

    private static final String ENTRY_TYPE = AdminApplication.class.getName();
    private final AdminApplicationController applicationController;
    private final boolean adminContext;

    /**
     * Creates JpaApplication Snapshot Data Store for streaming/applying {@link AdminApplication} data.
     *
     * @param replicationGroup      the application replicationGroup
     * @param applicationController the application controller used for retrieving/saving applications
     */
    public AdminApplicationSnapshotDataStore(String replicationGroup,
                                             AdminApplicationController applicationController) {
        this.applicationController = applicationController;
        this.adminContext = isAdmin(replicationGroup);
    }

    @Override
    public int order() {
        return 3;
    }

    @Override
    public Flux<SerializedObject> streamSnapshotData(SnapshotContext installationContext) {
        if (!adminContext) {
            return Flux.empty();
        }
        List<AdminApplication> applications = applicationController.getApplications();

        return Flux.fromIterable(applications)
                   .map(ApplicationProtoConverter::createApplication)
                   .map(this::toSerializedObject);
    }

    @Override
    public boolean canApplySnapshotData(String type) {
        return adminContext && ENTRY_TYPE.equals(type);
    }


    @Override
    public void applySnapshotData(SerializedObject serializedObject, Role role) {
        try {
            io.axoniq.axonserver.grpc.internal.Application applicationMessage = io.axoniq.axonserver.grpc.internal.Application
                    .parseFrom(serializedObject.getData());
            AdminApplication applicationEntity = createJpaApplication(applicationMessage);
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
