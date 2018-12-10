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
 * @author Milan Savic
 */
public class UserSnapshotDataProvider implements SnapshotDataProvider, SnapshotDataConsumer {

    private final String context;
    private final UserRepository userRepository;

    public UserSnapshotDataProvider(String context, UserRepository userRepository) {
        this.context = context;
        this.userRepository = userRepository;
    }

    @Override
    public int order() {
        return 10;
    }

    @Override
    public Flux<SerializedObject> provide(long from, long to) {
        return Flux.fromIterable(userRepository.findAll())
                   .map(ProtoConverter::createUser)
                   .map(this::toSerializedObject);
    }

    @Override
    public boolean canConsume(String type) {
        return User.class.getName().equals(type);
    }

    @Override
    public void consume(SerializedObject serializedObject) {
        try {
            io.axoniq.axonserver.grpc.internal.User userMessage = io.axoniq.axonserver.grpc.internal.User.parseFrom(
                    serializedObject.getData());
            User userEntity = createJpaUser(userMessage);
            userRepository.save(userEntity);
        } catch (InvalidProtocolBufferException e) {
            throw new SnapshotDeserializationException("Unable to deserialize user data.", e);
        }
    }

    private SerializedObject toSerializedObject(io.axoniq.axonserver.grpc.internal.User user) {
        return SerializedObject.newBuilder()
                               .setType(User.class.getName())
                               .setData(user.toByteString())
                               .build();
    }
}
