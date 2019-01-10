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
 * Snapshot data store for {@link Application} data.
 *
 * @author Milan Savic
 * @since 4.1
 */
public class ApplicationSnapshotDataStore implements SnapshotDataStore {

    private final String context;
    private final ApplicationController applicationController;

    /**
     * Creates Application Snapshot Data Store for streaming/applying {@link Application} data.
     *
     * @param context               the application context
     * @param applicationController the application controller used for retrieving/saving applications
     */
    public ApplicationSnapshotDataStore(String context, ApplicationController applicationController) {
        this.context = context;
        this.applicationController = applicationController;
    }

    @Override
    public int order() {
        return 0;
    }

    @Override
    public Flux<SerializedObject> streamSnapshotData(long fromEventSequence, long toEventSequence) {
        List<Application> applications = applicationController.getApplicationsForContext(context);

        return Flux.fromIterable(applications)
                   .map(ProtoConverter::createApplication)
                   .map(this::toSerializedObject);
    }

    @Override
    public boolean canApplySnapshotData(String type) {
        return Application.class.getName().equals(type);
    }

    @Override
    public void applySnapshotData(SerializedObject serializedObject) {
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
