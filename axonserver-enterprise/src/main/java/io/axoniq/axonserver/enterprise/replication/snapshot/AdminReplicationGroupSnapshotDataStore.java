package io.axoniq.axonserver.enterprise.replication.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.snapshot.SnapshotContext;
import io.axoniq.axonserver.cluster.snapshot.SnapshotDeserializationException;
import io.axoniq.axonserver.enterprise.jpa.AdminReplicationGroup;
import io.axoniq.axonserver.enterprise.replication.ReplicationGroupConfigurationMapping;
import io.axoniq.axonserver.enterprise.replication.admin.AdminReplicationGroupController;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.util.List;

import static io.axoniq.axonserver.RaftAdminGroup.isAdmin;


/**
 * Snapshot data store for {@link AdminReplicationGroup} data. This data is only distributed in the _admin context.
 *
 * @author Marc Gathier
 * @since 4.1.5
 */
public class AdminReplicationGroupSnapshotDataStore implements SnapshotDataStore {

    private static final Logger logger = LoggerFactory.getLogger(AdminReplicationGroupSnapshotDataStore.class);
    public static final String ENTRY_TYPE = ReplicationGroupConfiguration.class.getName();
    private final String replicationGroup;
    private final AdminReplicationGroupController adminReplicationGroupController;
    private final ReplicationGroupConfigurationMapping replicationGroupConfigurationMapping;
    private final boolean adminContext;

    /**
     * Creates Snapshot Data Store for streaming/applying {@link AdminReplicationGroup} data.
     *
     * @param replicationGroup                the application replicationGroup
     * @param adminReplicationGroupController the replicationGroup controller used for retrieving/saving applications
     */
    public AdminReplicationGroupSnapshotDataStore(String replicationGroup,
                                                  AdminReplicationGroupController adminReplicationGroupController) {
        this.replicationGroup = replicationGroup;
        this.adminReplicationGroupController = adminReplicationGroupController;
        this.adminContext = isAdmin(replicationGroup);
        this.replicationGroupConfigurationMapping = new ReplicationGroupConfigurationMapping();
    }

    @Override
    public int order() {
        return 1;
    }

    @Override
    public Flux<SerializedObject> streamSnapshotData(SnapshotContext installationContext) {
        if (!adminContext || !installationContext.supportsReplicationGroups()) {
            return Flux.empty();
        }
        List<AdminReplicationGroup> replicationGroups = adminReplicationGroupController.findAll();

        return Flux.fromIterable(replicationGroups)
                   .map(replicationGroupConfigurationMapping::apply)
                   .map(this::toSerializedObject);
    }

    @Override
    public boolean canApplySnapshotData(String type) {
        return adminContext && ENTRY_TYPE.equals(type);
    }


    @Override
    public void applySnapshotData(SerializedObject serializedObject, Role role) {
        try {
            ReplicationGroupConfiguration replicationGroupConfiguration = ReplicationGroupConfiguration.parseFrom(
                    serializedObject.getData());
            logger.debug("{}: register replication group {}", replicationGroup, replicationGroupConfiguration);
            adminReplicationGroupController.registerReplicationGroup(replicationGroupConfiguration);
        } catch (InvalidProtocolBufferException e) {
            throw new SnapshotDeserializationException("Unable to deserialize application data.", e);
        }
    }

    @Override
    public void clear() {
        if (adminContext) {
            logger.debug("{}: clear all replication groups from admin tables", replicationGroup);

            adminReplicationGroupController.unregisterAllAdminData();
        }
    }

    private SerializedObject toSerializedObject(ReplicationGroupConfiguration application) {
        return SerializedObject.newBuilder()
                               .setType(ENTRY_TYPE)
                               .setData(application.toByteString())
                               .build();
    }
}
