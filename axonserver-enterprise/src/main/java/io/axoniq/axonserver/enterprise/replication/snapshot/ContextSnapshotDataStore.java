package io.axoniq.axonserver.enterprise.replication.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.snapshot.SnapshotContext;
import io.axoniq.axonserver.cluster.snapshot.SnapshotDeserializationException;
import io.axoniq.axonserver.enterprise.jpa.AdminContext;
import io.axoniq.axonserver.enterprise.replication.group.ReplicationGroupController;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupContexts;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.stream.Collectors;

import static io.axoniq.axonserver.RaftAdminGroup.isAdmin;


/**
 * Snapshot data store for {@link io.axoniq.axonserver.enterprise.jpa.ReplicationGroupContext} data.
 *
 * @author Marc Gathier
 * @since 4.4
 */
public class ContextSnapshotDataStore implements SnapshotDataStore {

    public static final String ENTRY_TYPE = ReplicationGroupContexts.class.getName();
    private static final Logger logger = LoggerFactory.getLogger(ContextSnapshotDataStore.class);
    private final String replicationGroup;
    private final ReplicationGroupController replicationGroupController;
    private final LocalEventStore localEventStore;

    /**
     * Creates Context Snapshot Data Store for streaming/applying {@link AdminContext} data.
     *
     * @param replicationGroup           the replication group
     * @param replicationGroupController the replication group controller used for retrieving/saving contexts
     */
    public ContextSnapshotDataStore(String replicationGroup, ReplicationGroupController replicationGroupController,
                                    LocalEventStore localEventStore) {
        this.replicationGroup = replicationGroup;
        this.replicationGroupController = replicationGroupController;
        this.localEventStore = localEventStore;
    }

    @Override
    public int order() {
        return 10;
    }

    @Override
    public Flux<SerializedObject> streamSnapshotData(SnapshotContext installationContext) {
        List<io.axoniq.axonserver.enterprise.jpa.ReplicationGroupContext> contexts = replicationGroupController
                .findContextsByReplicationGroupName(replicationGroup);
        ReplicationGroupContexts replicationGroupContexts =
                ReplicationGroupContexts.newBuilder()
                                        .setReplicationGroupName(replicationGroup)
                                        .addAllContext(contexts.stream()
                                                               .map(replicationGroupContext ->
                                                                            io.axoniq.axonserver.grpc.internal.Context
                                                                                    .newBuilder()
                                                                                    .setContextName(
                                                                                            replicationGroupContext
                                                                                                    .getName())
                                                                                    .setFirstEventToken(firstEventToken(
                                                                                            replicationGroupContext
                                                                                                    .getName(),
                                                                                            installationContext.role()))
                                                                                    .setFirstSnapshotToken(
                                                                                            firstSnapshotToken(
                                                                                                    replicationGroupContext
                                                                                                            .getName(),
                                                                                                    installationContext
                                                                                                            .role()))
                                                                                    .putAllMetaData(
                                                                                            replicationGroupContext
                                                                                                    .getMetaDataMap())
                                                                                    .build())
                                                               .collect(Collectors
                                                                                .toSet()))
                                        .build();

        logger.debug("{}: sending context snapshot: {}", replicationGroup, replicationGroupContexts);
        return Flux.just(replicationGroupContexts)
                   .map(this::toSerializedObject);
    }

    private long firstEventToken(String contextName, Role role) {
        if (!isAdmin(contextName) && Role.PRIMARY.equals(role)) {
            return localEventStore.firstToken(contextName);
        }
        return 0L;
    }

    private long firstSnapshotToken(String contextName, Role role) {
        if (!isAdmin(contextName) && Role.PRIMARY.equals(role)) {
            return localEventStore.firstSnapshotToken(contextName);
        }
        return 0L;
    }

    @Override
    public boolean canApplySnapshotData(String type) {
        return ENTRY_TYPE.equals(type);
    }


    @Override
    public void applySnapshotData(SerializedObject serializedObject, Role role) {
        try {
            ReplicationGroupContexts contexts = ReplicationGroupContexts.parseFrom(serializedObject.getData());
            logger.debug("{}: received context snapshot: {}", replicationGroup, contexts);
            replicationGroupController.merge(contexts, role);
        } catch (InvalidProtocolBufferException e) {
            throw new SnapshotDeserializationException("Unable to deserialize application data.", e);
        }
    }

    @Override
    public void clear() {
        // no action here, apply will merge the information, as deleting a context and recreating it is
        // expensive operation.
    }

    private SerializedObject toSerializedObject(ReplicationGroupContexts application) {
        return SerializedObject.newBuilder()
                               .setType(ENTRY_TYPE)
                               .setData(application.toByteString())
                               .build();
    }
}
