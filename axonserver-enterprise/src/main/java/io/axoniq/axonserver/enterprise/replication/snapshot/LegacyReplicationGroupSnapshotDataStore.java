package io.axoniq.axonserver.enterprise.replication.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.snapshot.SnapshotContext;
import io.axoniq.axonserver.cluster.snapshot.SnapshotDeserializationException;
import io.axoniq.axonserver.enterprise.replication.ContextConfigurationMapping;
import io.axoniq.axonserver.enterprise.jpa.AdminContext;
import io.axoniq.axonserver.enterprise.jpa.AdminReplicationGroup;
import io.axoniq.axonserver.enterprise.replication.admin.AdminReplicationGroupController;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import io.axoniq.axonserver.grpc.internal.ContextConfiguration;
import reactor.core.publisher.Flux;

import java.util.List;

import static io.axoniq.axonserver.RaftAdminGroup.isAdmin;


/**
 * Snapshot data store for {@link ContextConfiguration} data received from/sending to nodes running pre 4.4 version.
 * This data is only distributed in the _admin context.
 *
 * @author Marc Gathier
 * @since 4.4
 */
public class LegacyReplicationGroupSnapshotDataStore implements SnapshotDataStore {

    public static final String ENTRY_TYPE = ContextConfiguration.class.getName();
    private final AdminReplicationGroupController adminReplicationGroupController;
    private final ContextConfigurationMapping contextConfigurationMapping;

    private final boolean adminContext;

    /**
     * Creates Context Snapshot Data Store for streaming/applying {@link AdminContext} data.
     *
     * @param replicationGroup                the application replicationGroup
     * @param adminReplicationGroupController the replicationGroup controller used for retrieving/saving applications
     */
    public LegacyReplicationGroupSnapshotDataStore(String replicationGroup,
                                                   AdminReplicationGroupController adminReplicationGroupController) {
        this.adminReplicationGroupController = adminReplicationGroupController;
        this.adminContext = isAdmin(replicationGroup);
        this.contextConfigurationMapping = new ContextConfigurationMapping();
    }

    @Override
    public int order() {
        return 1;
    }

    @Override
    public Flux<SerializedObject> streamSnapshotData(SnapshotContext installationContext) {
        if (!adminContext || installationContext.supportsReplicationGroups()) {
            return Flux.empty();
        }
        List<AdminReplicationGroup> replicationGroups = adminReplicationGroupController.findAll();

        return Flux.fromIterable(replicationGroups)
                   .map(contextConfigurationMapping::apply)
                   .map(this::toSerializedObject);
    }

    @Override
    public boolean canApplySnapshotData(String type) {
        return adminContext && ENTRY_TYPE.equals(type);
    }


    @Override
    public void applySnapshotData(SerializedObject serializedObject, Role role) {
        try {
            ContextConfiguration contextConfiguration = ContextConfiguration.parseFrom(
                    serializedObject.getData());
            adminReplicationGroupController.updateReplicationGroup(contextConfiguration);
        } catch (InvalidProtocolBufferException e) {
            throw new SnapshotDeserializationException("Unable to deserialize application data.", e);
        }
    }

    @Override
    public void clear() {
    }

    private SerializedObject toSerializedObject(ContextConfiguration application) {
        return SerializedObject.newBuilder()
                               .setType(ENTRY_TYPE)
                               .setData(application.toByteString())
                               .build();
    }
}
