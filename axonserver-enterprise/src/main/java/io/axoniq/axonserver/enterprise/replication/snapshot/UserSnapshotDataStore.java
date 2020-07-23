package io.axoniq.axonserver.enterprise.replication.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.access.user.ReplicationGroupUser;
import io.axoniq.axonserver.access.user.ReplicationGroupUserController;
import io.axoniq.axonserver.cluster.snapshot.SnapshotContext;
import io.axoniq.axonserver.cluster.snapshot.SnapshotDeserializationException;
import io.axoniq.axonserver.enterprise.replication.group.ReplicationGroupController;
import io.axoniq.axonserver.grpc.UserProtoConverter;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import io.axoniq.axonserver.grpc.internal.ContextUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;


/**
 * Snapshot data store for {@link ReplicationGroupUser} data.
 *
 * @author Marc Gathier
 * @since 4.2
 */
public class UserSnapshotDataStore implements SnapshotDataStore {

    private static final Logger logger = LoggerFactory.getLogger(UserSnapshotDataStore.class);
    private static final String TYPE = ReplicationGroupUser.class.getName();
    private final ReplicationGroupController replicationGroupController;
    private final ReplicationGroupUserController userController;
    private final String replicationGroup;

    /**
     * Creates Context User Snapshot Data Store for streaming/applying user data.
     *
     * @param replicationGroupController
     * @param userController             the repository for retrieving/saving users
     */
    public UserSnapshotDataStore(String replicationGroup,
                                 ReplicationGroupController replicationGroupController,
                                 ReplicationGroupUserController userController) {
        this.replicationGroupController = replicationGroupController;
        this.userController = userController;
        this.replicationGroup = replicationGroup;
    }

    @Override
    public int order() {
        return 12;
    }

    @Override
    public Flux<SerializedObject> streamSnapshotData(SnapshotContext installationContext) {
        return Flux.fromIterable(userController.getUsersForContexts(replicationGroupController
                                                                            .getContextNames(replicationGroup)))
                   .map(UserProtoConverter::createContextUser)
                   .map(this::toSerializedObject);
    }

    @Override
    public boolean canApplySnapshotData(String type) {
        return TYPE.equals(type);
    }

    @Override
    public void applySnapshotData(SerializedObject serializedObject, Role role) {
        try {
            ContextUser userMessage = ContextUser.parseFrom(
                    serializedObject.getData());
            ReplicationGroupUser userEntity = UserProtoConverter.createJpaContextUser(userMessage);
            userController.save(userEntity);
        } catch (InvalidProtocolBufferException e) {
            throw new SnapshotDeserializationException("Unable to deserialize user data.", e);
        }
    }

    @Override
    public void clear() {
        if (logger.isDebugEnabled()) {
            logger.debug("{}: Clearing users for contexts: {}",
                         replicationGroup,
                         replicationGroupController.getContextNames(replicationGroup));
        }
        replicationGroupController.getContextNames(replicationGroup).forEach(userController::deleteByContext);
    }

    private SerializedObject toSerializedObject(ContextUser user) {
        return SerializedObject.newBuilder()
                               .setType(TYPE)
                               .setData(user.toByteString())
                               .build();
    }
}
