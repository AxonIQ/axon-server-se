package io.axoniq.axonserver.enterprise.replication.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.access.jpa.User;
import io.axoniq.axonserver.access.user.UserRepository;
import io.axoniq.axonserver.cluster.snapshot.SnapshotContext;
import io.axoniq.axonserver.cluster.snapshot.SnapshotDeserializationException;
import io.axoniq.axonserver.grpc.UserProtoConverter;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import reactor.core.publisher.Flux;

import static io.axoniq.axonserver.RaftAdminGroup.isAdmin;


/**
 * Snapshot data store for {@link User} data.
 *
 * @author Milan Savic
 * @since 4.1
 */
public class AdminUserSnapshotDataStore implements SnapshotDataStore {

    private static final String TYPE = User.class.getName();
    private final UserRepository userRepository;
    private final boolean adminContext;

    /**
     * Creates User Snapshot Data Store for streaming/applying user data.
     *
     * @param userRepository the repository for retrieving/saving users
     */
    public AdminUserSnapshotDataStore(String replicationGroup, UserRepository userRepository) {
        this.userRepository = userRepository;
        this.adminContext = isAdmin(replicationGroup);
    }

    @Override
    public int order() {
        return 4;
    }

    @Override
    public Flux<SerializedObject> streamSnapshotData(SnapshotContext installationContext) {
        if (!adminContext) {
            return Flux.empty();
        }
        return Flux.fromIterable(userRepository.findAll())
                   .map(UserProtoConverter::createUser)
                   .map(this::toSerializedObject);
    }

    @Override
    public boolean canApplySnapshotData(String type) {
        return adminContext && TYPE.equals(type);
    }

    @Override
    public void applySnapshotData(SerializedObject serializedObject, Role role) {
        try {
            io.axoniq.axonserver.grpc.internal.User userMessage = io.axoniq.axonserver.grpc.internal.User.parseFrom(
                    serializedObject.getData());
            User userEntity = UserProtoConverter.createJpaUser(userMessage);
            userRepository.save(userEntity);
        } catch (InvalidProtocolBufferException e) {
            throw new SnapshotDeserializationException("Unable to deserialize user data.", e);
        }
    }

    @Override
    public void clear() {
        if( adminContext) {
            userRepository.deleteAll();
        }
    }

    private SerializedObject toSerializedObject(io.axoniq.axonserver.grpc.internal.User user) {
        return SerializedObject.newBuilder()
                               .setType(TYPE)
                               .setData(user.toByteString())
                               .build();
    }
}
