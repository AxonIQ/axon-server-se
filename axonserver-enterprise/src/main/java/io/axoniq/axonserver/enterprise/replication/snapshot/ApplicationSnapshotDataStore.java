package io.axoniq.axonserver.enterprise.replication.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.access.application.ReplicationGroupApplication;
import io.axoniq.axonserver.access.application.ReplicationGroupApplicationController;
import io.axoniq.axonserver.cluster.snapshot.SnapshotContext;
import io.axoniq.axonserver.cluster.snapshot.SnapshotDeserializationException;
import io.axoniq.axonserver.enterprise.replication.group.ReplicationGroupController;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import io.axoniq.axonserver.grpc.internal.ContextApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.util.List;


/**
 * Snapshot data store for {@link ReplicationGroupApplication} data. This data is distributed for all replication
 * groups.
 *
 * @author Marc Gathier
 * @since 4.1
 */
public class ApplicationSnapshotDataStore implements SnapshotDataStore {

    private static final Logger logger = LoggerFactory.getLogger(ApplicationSnapshotDataStore.class);
    private static final String TYPE = ReplicationGroupApplication.class.getName();
    private final String replicationGroup;
    private final ReplicationGroupController replicationGroupController;
    private final ReplicationGroupApplicationController applicationController;

    /**
     * Creates JpaApplication Snapshot Data Store for streaming/applying {@link ReplicationGroupApplication} data.
     *
     * @param replicationGroup      the application context
     * @param applicationController the application controller used for retrieving/saving applications
     */
    public ApplicationSnapshotDataStore(String replicationGroup,
                                        ReplicationGroupController replicationGroupController,
                                        ReplicationGroupApplicationController applicationController) {
        this.replicationGroup = replicationGroup;
        this.replicationGroupController = replicationGroupController;
        this.applicationController = applicationController;
    }

    @Override
    public int order() {
        return 11;
    }

    @Override
    public Flux<SerializedObject> streamSnapshotData(SnapshotContext installationContext) {
        List<ContextApplication> applications = applicationController.getApplicationsForContexts(
                replicationGroupController.getContextNames(replicationGroup));

        return Flux.fromIterable(applications)
                   .map(this::toSerializedObject);
    }

    @Override
    public boolean canApplySnapshotData(String type) {
        return TYPE.equals(type);
    }


    @Override
    public void applySnapshotData(SerializedObject serializedObject, Role role) {
        try {
            ContextApplication applicationMessage = ContextApplication.parseFrom(serializedObject.getData());
            applicationController.mergeApplication(applicationMessage);
        } catch (InvalidProtocolBufferException e) {
            throw new SnapshotDeserializationException("Unable to deserialize application data.", e);
        }
    }

    @Override
    public void clear() {
        if (logger.isDebugEnabled()) {
            logger.debug("{}: Clearing applications for contexts: {}",
                         replicationGroup,
                         replicationGroupController.getContextNames(replicationGroup));
        }
        replicationGroupController.getContextNames(replicationGroup).forEach(applicationController::deleteByContext);
    }

    private SerializedObject toSerializedObject(ContextApplication application) {
        return SerializedObject.newBuilder()
                               .setType(TYPE)
                               .setData(application.toByteString())
                               .build();
    }
}
