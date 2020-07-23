package io.axoniq.axonserver.enterprise.replication.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.snapshot.SnapshotContext;
import io.axoniq.axonserver.cluster.snapshot.SnapshotDeserializationException;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.ReplicationGroupProcessorLoadBalancing;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.ReplicationGroupProcessorLoadBalancingRepository;
import io.axoniq.axonserver.enterprise.replication.group.ReplicationGroupController;
import io.axoniq.axonserver.grpc.ProcessorLBStrategyConverter;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import static io.axoniq.axonserver.grpc.ProcessorLBStrategyConverter.createJpaRaftProcessorLoadBalancing;


/**
 * Snapshot data store for {@link ReplicationGroupProcessorLoadBalancing} data.
 *
 * @author Marc Gathier
 * @since 4.2
 */
public class ProcessorLoadBalancingSnapshotDataStore implements SnapshotDataStore {

    private static final Logger logger = LoggerFactory.getLogger(ProcessorLoadBalancingSnapshotDataStore.class);
    private final String replicationGroup;
    private final ReplicationGroupController replicationGroupController;
    private final ReplicationGroupProcessorLoadBalancingRepository processorLoadBalancingRepository;

    /**
     * Creates Processor Load Balancing Snapshot Data Store for streaming/applying processor load balancing data.
     *
     * @param replicationGroup                 application context
     * @param replicationGroupController
     * @param processorLoadBalancingRepository the repository for retrieving/saving processor load balancing data
     */
    public ProcessorLoadBalancingSnapshotDataStore(
            String replicationGroup,
            ReplicationGroupController replicationGroupController,
            ReplicationGroupProcessorLoadBalancingRepository processorLoadBalancingRepository) {
        this.replicationGroup = replicationGroup;
        this.replicationGroupController = replicationGroupController;
        this.processorLoadBalancingRepository = processorLoadBalancingRepository;
    }

    @Override
    public int order() {
        return 30;
    }

    @Override
    public Flux<SerializedObject> streamSnapshotData(SnapshotContext installationContext) {
        return Flux.fromIterable(processorLoadBalancingRepository
                                         .findByContext(replicationGroupController.getContextNames(replicationGroup)))
                   .map(ProcessorLBStrategyConverter::createProcessorLBStrategy)
                   .map(this::toSerializedObject);
    }

    @Override
    public boolean canApplySnapshotData(String type) {
        return ReplicationGroupProcessorLoadBalancing.class.getName().equals(type);
    }

    @Override
    public void applySnapshotData(SerializedObject serializedObject, Role role) {
        try {
            ProcessorLBStrategy processorLBStrategy = ProcessorLBStrategy.parseFrom(serializedObject.getData());
            ReplicationGroupProcessorLoadBalancing processorLoadBalancingEntity = createJpaRaftProcessorLoadBalancing(
                    processorLBStrategy);
            processorLoadBalancingRepository.save(processorLoadBalancingEntity);
        } catch (InvalidProtocolBufferException e) {
            throw new SnapshotDeserializationException("Unable to deserialize processor load balancing data.", e);
        }
    }

    @Override
    public void clear() {
        if (logger.isDebugEnabled()) {
            logger.debug("{}: Clearing processor load balance configuration for contexts: {}",
                         replicationGroup,
                         replicationGroupController.getContextNames(replicationGroup));
        }
        replicationGroupController.getContextNames(replicationGroup)
                                  .forEach(processorLoadBalancingRepository::deleteAllByProcessorContext);
    }

    private SerializedObject toSerializedObject(ProcessorLBStrategy processorLBStrategy) {
        return SerializedObject.newBuilder()
                               .setType(ReplicationGroupProcessorLoadBalancing.class.getName())
                               .setData(processorLBStrategy.toByteString())
                               .build();
    }
}
