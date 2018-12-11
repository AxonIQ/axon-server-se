package io.axoniq.axonserver.enterprise.cluster.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.snapshot.SnapshotDeserializationException;
import io.axoniq.axonserver.grpc.ProtoConverter;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import io.axoniq.platform.application.jpa.Application;
import reactor.core.publisher.Flux;

import java.util.List;
import javax.persistence.EntityManager;

import static io.axoniq.axonserver.grpc.ProtoConverter.createJpaApplication;

/**
 * @author Milan Savic
 */
public class ApplicationSnapshotDataProvider implements SnapshotDataProvider {

    private final String context;
    private final EntityManager entityManager;

    public ApplicationSnapshotDataProvider(String context, EntityManager entityManager) {
        this.context = context;
        this.entityManager = entityManager;
    }

    @Override
    public int order() {
        return 0;
    }

    @Override
    public Flux<SerializedObject> provide(long from, long to) {
        List<Application> applications = entityManager.createQuery("select app from io.axoniq.platform.application.jpa.Application app " +
                                                                           "left join app.contexts app_context " +
                                                                           "where app_context.context = :context",
                                                                   Application.class)
                                                      .setParameter("context", context)
                                                      .getResultList();

        return Flux.fromIterable(applications)
                   .map(ProtoConverter::createApplication)
                   .map(this::toSerializedObject);
    }

    @Override
    public boolean canConsume(String type) {
        return Application.class.getName().equals(type);
    }

    @Override
    public void consume(SerializedObject serializedObject) {
        try {
            io.axoniq.axonserver.grpc.internal.Application applicationMessage = io.axoniq.axonserver.grpc.internal.Application
                    .parseFrom(serializedObject.getData());
            Application applicationEntity = createJpaApplication(applicationMessage);
            entityManager.merge(applicationEntity);
        } catch (InvalidProtocolBufferException e) {
            throw new SnapshotDeserializationException("Unable to deserialize application data.", e);
        }
    }

    private SerializedObject toSerializedObject(io.axoniq.axonserver.grpc.internal.Application application) {
        return SerializedObject.newBuilder()
                               .setType(Application.class.getName())
                               .setData(application.toByteString())
                               .build();
    }
}
