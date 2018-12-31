package io.axoniq.axonserver.enterprise.cluster.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.snapshot.SnapshotDeserializationException;
import io.axoniq.axonserver.grpc.ProtoConverter;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import io.axoniq.platform.application.ApplicationController;
import io.axoniq.platform.application.jpa.Application;
import reactor.core.publisher.Flux;

import java.util.List;

import static io.axoniq.axonserver.grpc.ProtoConverter.createJpaApplication;

/**
 * @author Milan Savic
 */
public class ApplicationSnapshotDataProvider implements SnapshotDataProvider {

    private final String context;
    private final ApplicationController applicationController;

    public ApplicationSnapshotDataProvider(String context, ApplicationController applicationController) {
        this.context = context;
        this.applicationController = applicationController;
    }

    @Override
    public int order() {
        return 0;
    }

    @Override
    public Flux<SerializedObject> provide(long from, long to) {
        List<Application> applications = applicationController.getApplicationsForContext(context);

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
            applicationController.mergeContext(applicationEntity, context);
        } catch (InvalidProtocolBufferException e) {
            throw new SnapshotDeserializationException("Unable to deserialize application data.", e);
        }
    }

    @Override
    public void clear() {
        applicationController.deleteByContext(context);
    }

    private SerializedObject toSerializedObject(io.axoniq.axonserver.grpc.internal.Application application) {
        return SerializedObject.newBuilder()
                               .setType(Application.class.getName())
                               .setData(application.toByteString())
                               .build();
    }
}
