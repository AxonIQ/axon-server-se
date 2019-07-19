package io.axoniq.axonserver.enterprise.cluster.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.snapshot.SnapshotContext;
import io.axoniq.axonserver.cluster.snapshot.SnapshotDeserializationException;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.RaftProcessorLoadBalancing;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.RaftProcessorLoadBalancingRepository;
import io.axoniq.axonserver.grpc.ProcessorLBStrategyConverter;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;
import reactor.core.publisher.Flux;

import static io.axoniq.axonserver.grpc.ProcessorLBStrategyConverter.createJpaRaftProcessorLoadBalancing;


/**
 * Snapshot data store for {@link RaftProcessorLoadBalancing} data.
 *
 * @author Milan Savic
 * @since 4.1
 */
public class RaftProcessorLoadBalancingSnapshotDataStore implements SnapshotDataStore {

    private final String context;
    private final RaftProcessorLoadBalancingRepository processorLoadBalancingRepository;

    /**
     * Creates Processor Load Balancing Snapshot Data Store for streaming/applying processor load balancing data.
     *
     * @param context                          application context
     * @param processorLoadBalancingRepository the repository for retrieving/saving processor load balancing data
     */
    public RaftProcessorLoadBalancingSnapshotDataStore(
            String context, RaftProcessorLoadBalancingRepository processorLoadBalancingRepository) {
        this.context = context;
        this.processorLoadBalancingRepository = processorLoadBalancingRepository;
    }

    @Override
    public int order() {
        return 30;
    }

    @Override
    public Flux<SerializedObject> streamSnapshotData(SnapshotContext installationContext) {
        return Flux.fromIterable(processorLoadBalancingRepository.findByContext(context))
                   .map(ProcessorLBStrategyConverter::createProcessorLBStrategy)
                   .map(this::toSerializedObject);
    }

    @Override
    public boolean canApplySnapshotData(String type) {
        return RaftProcessorLoadBalancing.class.getName().equals(type);
    }

    @Override
    public void applySnapshotData(SerializedObject serializedObject) {
        try {
            ProcessorLBStrategy processorLBStrategy = ProcessorLBStrategy.parseFrom(serializedObject.getData());
            RaftProcessorLoadBalancing processorLoadBalancingEntity = createJpaRaftProcessorLoadBalancing(
                    processorLBStrategy);
            processorLoadBalancingRepository.save(processorLoadBalancingEntity);
        } catch (InvalidProtocolBufferException e) {
            throw new SnapshotDeserializationException("Unable to deserialize processor load balancing data.", e);
        }
    }

    @Override
    public void clear() {
        processorLoadBalancingRepository.deleteAllByProcessorContext(context);
    }

    private SerializedObject toSerializedObject(ProcessorLBStrategy processorLBStrategy) {
        return SerializedObject.newBuilder()
                               .setType(RaftProcessorLoadBalancing.class.getName())
                               .setData(processorLBStrategy.toByteString())
                               .build();
    }
}
