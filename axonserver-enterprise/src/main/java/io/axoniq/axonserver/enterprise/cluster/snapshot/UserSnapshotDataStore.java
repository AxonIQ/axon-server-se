package io.axoniq.axonserver.enterprise.cluster.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.snapshot.SnapshotDeserializationException;
import io.axoniq.axonserver.grpc.ProtoConverter;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import io.axoniq.platform.user.User;
import io.axoniq.platform.user.UserRepository;
import reactor.core.publisher.Flux;

import static io.axoniq.axonserver.grpc.ProtoConverter.createJpaUser;

/**
 * Snapshot data store for {@link User} data.
 *
 * @author Milan Savic
 * @since 4.1
 */
public class UserSnapshotDataStore implements SnapshotDataStore {

    private final UserRepository userRepository;

    /**
     * Creates User Snapshot Data Store for streaming/applying user data.
     *
     * @param userRepository the repository for retrieving/saving users
     */
    public UserSnapshotDataStore(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @Override
    public int order() {
        return 10;
    }

    @Override
    public Flux<SerializedObject> streamSnapshotData(long fromEventSequence, long toEventSequence) {
        return Flux.fromIterable(userRepository.findAll())
                   .map(ProtoConverter::createUser)
                   .map(this::toSerializedObject);
    }

    @Override
    public boolean canApplySnapshotData(String type) {
        return User.class.getName().equals(type);
    }

    @Override
    public void applySnapshotData(SerializedObject serializedObject) {
        try {
            io.axoniq.axonserver.grpc.internal.User userMessage = io.axoniq.axonserver.grpc.internal.User.parseFrom(
                    serializedObject.getData());
            User userEntity = createJpaUser(userMessage);
            userRepository.save(userEntity);
        } catch (InvalidProtocolBufferException e) {
            throw new SnapshotDeserializationException("Unable to deserialize user data.", e);
        }
    }

    @Override
    public void clear() {
        // TODO: 1/10/2019 do we create users per context?
        userRepository.deleteAll();
    }

    private SerializedObject toSerializedObject(io.axoniq.axonserver.grpc.internal.User user) {
        return SerializedObject.newBuilder()
                               .setType(User.class.getName())
                               .setData(user.toByteString())
                               .build();
    }
}
