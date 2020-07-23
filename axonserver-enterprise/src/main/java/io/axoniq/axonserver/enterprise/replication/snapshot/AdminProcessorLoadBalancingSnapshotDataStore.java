package io.axoniq.axonserver.enterprise.replication.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.snapshot.SnapshotContext;
import io.axoniq.axonserver.cluster.snapshot.SnapshotDeserializationException;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.AdminProcessorLoadBalancing;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.AdminProcessorLoadBalancingRepository;
import io.axoniq.axonserver.grpc.ProcessorLBStrategyConverter;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;
import reactor.core.publisher.Flux;

import static io.axoniq.axonserver.RaftAdminGroup.isAdmin;
import static io.axoniq.axonserver.grpc.ProcessorLBStrategyConverter.createJpaProcessorLoadBalancing;


/**
 * Snapshot data store for {@link AdminProcessorLoadBalancing} data. This information is only replicated between admin
 * nodes.
 *
 * @author Milan Savic
 * @since 4.1
 */
public class AdminProcessorLoadBalancingSnapshotDataStore implements SnapshotDataStore {

    private final AdminProcessorLoadBalancingRepository processorLoadBalancingRepository;
    private final boolean adminContext;

    /**
     * Creates Processor Load Balancing Snapshot Data Store for streaming/applying processor load balancing data.
     *
     * @param replicationGroup                 application replicationGroup
     * @param processorLoadBalancingRepository the repository for retrieving/saving processor load balancing data
     */
    public AdminProcessorLoadBalancingSnapshotDataStore(
            String replicationGroup, AdminProcessorLoadBalancingRepository processorLoadBalancingRepository) {
        this.processorLoadBalancingRepository = processorLoadBalancingRepository;
        this.adminContext = isAdmin(replicationGroup);
    }

    @Override
    public int order() {
        return 5;
    }

    @Override
    public Flux<SerializedObject> streamSnapshotData(SnapshotContext installationContext) {
        return Flux.fromIterable(processorLoadBalancingRepository.findAll())
                   .map(ProcessorLBStrategyConverter::createProcessorLBStrategy)
                   .map(this::toSerializedObject);
    }

    @Override
    public boolean canApplySnapshotData(String type) {
        return adminContext && AdminProcessorLoadBalancing.class.getName().equals(type);
    }

    @Override
    public void applySnapshotData(SerializedObject serializedObject, Role role) {
        try {
            ProcessorLBStrategy processorLBStrategy = ProcessorLBStrategy.parseFrom(serializedObject.getData());
            AdminProcessorLoadBalancing processorLoadBalancingEntity = createJpaProcessorLoadBalancing(
                    processorLBStrategy);
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
                               .setType(AdminProcessorLoadBalancing.class.getName())
                               .setData(processorLBStrategy.toByteString())
                               .build();
    }
}
