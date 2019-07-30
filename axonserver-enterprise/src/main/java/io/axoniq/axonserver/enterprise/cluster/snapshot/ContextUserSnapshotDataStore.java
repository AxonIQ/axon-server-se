package io.axoniq.axonserver.enterprise.cluster.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.access.application.JpaContextUser;
import io.axoniq.axonserver.access.application.JpaContextUserController;
import io.axoniq.axonserver.cluster.snapshot.SnapshotContext;
import io.axoniq.axonserver.cluster.snapshot.SnapshotDeserializationException;
import io.axoniq.axonserver.grpc.UserProtoConverter;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import io.axoniq.axonserver.grpc.internal.ContextUser;
import reactor.core.publisher.Flux;


/**
 * Snapshot data store for {@link io.axoniq.axonserver.access.application.JpaContextUser} data.
 *
 * @author Marc Gathier
 * @since 4.2
 */
public class ContextUserSnapshotDataStore implements SnapshotDataStore {

    private static final String TYPE = JpaContextUser.class.getName();
    private final JpaContextUserController userRepository;
    private final String context;

    /**
     * Creates Context User Snapshot Data Store for streaming/applying user data.
     *
     * @param userRepository the repository for retrieving/saving users
     */
    public ContextUserSnapshotDataStore(String context, JpaContextUserController userRepository) {
        this.userRepository = userRepository;
        this.context = context;
    }

    @Override
    public int order() {
        return 10;
    }

    @Override
    public Flux<SerializedObject> streamSnapshotData(SnapshotContext installationContext) {
        return Flux.fromIterable(userRepository.getUsersForContext(context))
                   .map(UserProtoConverter::createContextUser)
                   .map(this::toSerializedObject);
    }

    @Override
    public boolean canApplySnapshotData(String type) {
        return TYPE.equals(type);
    }

    @Override
    public void applySnapshotData(SerializedObject serializedObject) {
        try {
            ContextUser userMessage = ContextUser.parseFrom(
                    serializedObject.getData());
            JpaContextUser userEntity = UserProtoConverter.createJpaContextUser(userMessage);
            userRepository.save(userEntity);
        } catch (InvalidProtocolBufferException e) {
            throw new SnapshotDeserializationException("Unable to deserialize user data.", e);
        }
    }

    @Override
    public void clear() {
        userRepository.deleteByContext(context);
    }

    private SerializedObject toSerializedObject(ContextUser user) {
        return SerializedObject.newBuilder()
                               .setType(TYPE)
                               .setData(user.toByteString())
                               .build();
    }
}
