package io.axoniq.axonserver.enterprise.cluster.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.snapshot.SnapshotDeserializationException;
import io.axoniq.axonserver.cluster.snapshot.SnapshotContext;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.ProcessorLoadBalancing;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.ProcessorLoadBalancingRepository;
import io.axoniq.axonserver.grpc.ProcessorLBStrategyConverter;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;
import reactor.core.publisher.Flux;

import static io.axoniq.axonserver.RaftAdminGroup.isAdmin;
import static io.axoniq.axonserver.grpc.ProcessorLBStrategyConverter.createJpaProcessorLoadBalancing;


/**
 * Snapshot data store for {@link ProcessorLoadBalancing} data. This information is only replicated between admin nodes.
 *
 * @author Milan Savic
 * @since 4.1
 */
public class ProcessorLoadBalancingSnapshotDataStore implements SnapshotDataStore {

    private final ProcessorLoadBalancingRepository processorLoadBalancingRepository;
    private final boolean adminContext;

    /**
     * Creates Processor Load Balancing Snapshot Data Store for streaming/applying processor load balancing data.
     *
     * @param context                          application context
     * @param processorLoadBalancingRepository the repository for retrieving/saving processor load balancing data
     */
    public ProcessorLoadBalancingSnapshotDataStore(
            String context, ProcessorLoadBalancingRepository processorLoadBalancingRepository) {
        this.processorLoadBalancingRepository = processorLoadBalancingRepository;
        this.adminContext = isAdmin(context);
    }

    @Override
    public int order() {
        return 30;
    }

    @Override
    public Flux<SerializedObject> streamSnapshotData(SnapshotContext installationContext) {
        return Flux.fromIterable(processorLoadBalancingRepository.findAll())
                   .map(ProcessorLBStrategyConverter::createProcessorLBStrategy)
                   .map(this::toSerializedObject);
    }

    @Override
    public boolean canApplySnapshotData(String type) {
        return adminContext && ProcessorLoadBalancing.class.getName().equals(type);
    }

    @Override
    public void applySnapshotData(SerializedObject serializedObject) {
        try {
            ProcessorLBStrategy processorLBStrategy = ProcessorLBStrategy.parseFrom(serializedObject.getData());
            ProcessorLoadBalancing processorLoadBalancingEntity = createJpaProcessorLoadBalancing(processorLBStrategy);
            processorLoadBalancingRepository.save(processorLoadBalancingEntity);
        } catch (InvalidProtocolBufferException e) {
            throw new SnapshotDeserializationException("Unable to deserialize processor load balancing data.", e);
        }
    }

    @Override
    public void clear() {
        if (adminContext) {
            processorLoadBalancingRepository.deleteAll();
        }
    }

    private SerializedObject toSerializedObject(ProcessorLBStrategy processorLBStrategy) {
        return SerializedObject.newBuilder()
                               .setType(ProcessorLoadBalancing.class.getName())
                               .setData(processorLBStrategy.toByteString())
                               .build();
    }
}
