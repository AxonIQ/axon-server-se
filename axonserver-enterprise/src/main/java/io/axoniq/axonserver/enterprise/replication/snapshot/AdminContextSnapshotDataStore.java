package io.axoniq.axonserver.enterprise.replication.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.snapshot.SnapshotContext;
import io.axoniq.axonserver.cluster.snapshot.SnapshotDeserializationException;
import io.axoniq.axonserver.enterprise.jpa.AdminContext;
import io.axoniq.axonserver.enterprise.jpa.AdminReplicationGroup;
import io.axoniq.axonserver.enterprise.replication.admin.AdminReplicationGroupController;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupContexts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.stream.Collectors;

import static io.axoniq.axonserver.RaftAdminGroup.isAdmin;


/**
 * Snapshot data store for {@link AdminContext} data. This data is only distributed in the _admin context.
 *
 * @author Marc Gathier
 * @since 4.1.5
 */
public class AdminContextSnapshotDataStore implements SnapshotDataStore {

    public static final String ENTRY_TYPE = "ADMIN_REPLICATION_GROUP_CONTEXTS";
    private static final Logger logger = LoggerFactory.getLogger(AdminContextSnapshotDataStore.class);
    private final String replicationGroup;
    private final AdminReplicationGroupController replicationGroupController;
    private final boolean adminContext;

    /**
     * Creates Context Snapshot Data Store for streaming/applying {@link AdminContext} data.
     *
     * @param replicationGroup           the application context
     * @param replicationGroupController the context controller used for retrieving/saving applications
     */
    public AdminContextSnapshotDataStore(String replicationGroup,
                                         AdminReplicationGroupController replicationGroupController) {
        this.replicationGroup = replicationGroup;
        this.replicationGroupController = replicationGroupController;
        this.adminContext = isAdmin(replicationGroup);
    }

    @Override
    public int order() {
        return 2;
    }

    @Override
    public Flux<SerializedObject> streamSnapshotData(SnapshotContext installationContext) {
        if (!adminContext || !installationContext.supportsReplicationGroups()) {
            return Flux.empty();
        }
        List<AdminReplicationGroup> contexts = replicationGroupController.findAll();
        return Flux.fromIterable(contexts)
                   .map(this::toReplicationGroupContexts)
                   .map(this::toSerializedObject);
    }

    private ReplicationGroupContexts toReplicationGroupContexts(AdminReplicationGroup replicationGroup) {
        return ReplicationGroupContexts.newBuilder()
                                       .setReplicationGroupName(
                                               replicationGroup.getName())
                                       .addAllContext(
                                               replicationGroup.getContexts().stream()
                                                               .map(replicationGroupContext ->
                                                                            io.axoniq.axonserver.grpc.internal.Context
                                                                                    .newBuilder()
                                                                                    .setContextName(
                                                                                            replicationGroupContext
                                                                                                    .getName())
                                                                                    .putAllMetaData(
                                                                                            replicationGroupContext
                                                                                                    .getMetaDataMap())
                                                                                    .build())
                                                               .collect(Collectors
                                                                                .toSet()))
                                       .build();
    }

    @Override
    public boolean canApplySnapshotData(String type) {
        return adminContext && ENTRY_TYPE.equals(type);
    }


    @Override
    public void applySnapshotData(SerializedObject serializedObject, Role role) {
        try {
            ReplicationGroupContexts contexts = ReplicationGroupContexts.parseFrom(serializedObject.getData());
            logger.debug("{}: add context group {}", replicationGroup, contexts);
            replicationGroupController.registerContexts(contexts);
        } catch (InvalidProtocolBufferException e) {
            throw new SnapshotDeserializationException("Unable to deserialize application data.", e);
        }
    }

    @Override
    public void clear() {
        if (adminContext) {
            replicationGroupController.unregisterAllContexts();
        }
    }

    private SerializedObject toSerializedObject(ReplicationGroupContexts application) {
        return SerializedObject.newBuilder()
                               .setType(ENTRY_TYPE)
                               .setData(application.toByteString())
                               .build();
    }
}
