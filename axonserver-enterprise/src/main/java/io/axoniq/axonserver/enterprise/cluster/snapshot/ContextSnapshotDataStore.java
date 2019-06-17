package io.axoniq.axonserver.enterprise.cluster.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.snapshot.SnapshotContext;
import io.axoniq.axonserver.cluster.snapshot.SnapshotDeserializationException;
import io.axoniq.axonserver.enterprise.cluster.internal.ContextConfigurationMapping;
import io.axoniq.axonserver.enterprise.context.ContextController;
import io.axoniq.axonserver.enterprise.jpa.Context;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import io.axoniq.axonserver.grpc.internal.ContextConfiguration;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.stream.Collectors;

import static io.axoniq.axonserver.RaftAdminGroup.isAdmin;


/**
 * Snapshot data store for {@link Context} data. This data is only distributed in the _admin context.
 *
 * @author Marc Gathier
 * @since 4.1.5
 */
public class ContextSnapshotDataStore implements SnapshotDataStore {

    public static final String ENTRY_TYPE = ContextConfiguration.class.getName();
    private final ContextController contextController;
    private final ContextConfigurationMapping contextConfigurationMapping;
    private final boolean adminContext;

    /**
     * Creates Context Snapshot Data Store for streaming/applying {@link Context} data.
     *
     * @param context               the application context
     * @param contextController     the context controller used for retrieving/saving applications
     */
    public ContextSnapshotDataStore(String context, ContextController contextController) {
        this.contextController = contextController;
        this.adminContext = isAdmin(context);
        this.contextConfigurationMapping = new ContextConfigurationMapping();
    }

    @Override
    public int order() {
        return 0;
    }

    @Override
    public Flux<SerializedObject> streamSnapshotData(SnapshotContext installationContext) {
        if(! adminContext) return Flux.empty();
        List<Context> applications = contextController.getContexts().collect(Collectors.toList());

        return Flux.fromIterable(applications)
                   .map(contextConfigurationMapping::apply)
                   .map(this::toSerializedObject);
    }

    @Override
    public boolean canApplySnapshotData(String type) {
        return adminContext && ENTRY_TYPE.equals(type);
    }


    @Override
    public void applySnapshotData(SerializedObject serializedObject) {
        try {
            io.axoniq.axonserver.grpc.internal.ContextConfiguration contextConfiguration = io.axoniq.axonserver.grpc.internal.ContextConfiguration
                    .parseFrom(serializedObject.getData());
            contextController.updateContext(contextConfiguration);
        } catch (InvalidProtocolBufferException e) {
            throw new SnapshotDeserializationException("Unable to deserialize application data.", e);
        }
    }

    @Override
    public void clear() {
        if( adminContext) {
            contextController.deleteAll();
        }
    }

    private SerializedObject toSerializedObject(io.axoniq.axonserver.grpc.internal.ContextConfiguration application) {
        return SerializedObject.newBuilder()
                               .setType(ENTRY_TYPE)
                               .setData(application.toByteString())
                               .build();
    }
}
