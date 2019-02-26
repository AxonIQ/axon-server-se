package io.axoniq.axonserver.enterprise.cluster.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.snapshot.SnapshotDeserializationException;
import io.axoniq.axonserver.cluster.snapshot.SnapshotContext;
import io.axoniq.axonserver.component.processor.balancing.jpa.LoadBalancingStrategy;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.LoadBalanceStrategyRepository;
import io.axoniq.axonserver.grpc.LoadBalancingStrategyConverter;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import io.axoniq.axonserver.grpc.internal.LoadBalanceStrategy;
import reactor.core.publisher.Flux;


/**
 * Snapshot data store for {@link LoadBalancingStrategy} data.
 *
 * @author Milan Savic
 * @since 4.1
 */
public class LoadBalanceStrategySnapshotDataStore implements SnapshotDataStore {

    private final LoadBalanceStrategyRepository loadBalanceStrategyRepository;

    /**
     * Creates Load Balance Strategy Snapshot Data Store for streaming/applying load balance strategy data.
     *
     * @param loadBalanceStrategyRepository the repository for retrieving/saving load balance strategies
     */
    public LoadBalanceStrategySnapshotDataStore(LoadBalanceStrategyRepository loadBalanceStrategyRepository) {
        this.loadBalanceStrategyRepository = loadBalanceStrategyRepository;
    }

    @Override
    public Flux<SerializedObject> streamSnapshotData(SnapshotContext installationContext) {
        return Flux.fromIterable(loadBalanceStrategyRepository.findAll())
                   .map(LoadBalancingStrategyConverter::createLoadBalanceStrategy)
                   .map(this::toSerializedObject);
    }

    @Override
    public int order() {
        return 20;
    }

    @Override
    public boolean canApplySnapshotData(String type) {
        return LoadBalancingStrategy.class.getName().equals(type);
    }

    @Override
    public void applySnapshotData(SerializedObject serializedObject) {
        try {
            LoadBalanceStrategy loadBalanceStrategy = LoadBalanceStrategy.parseFrom(serializedObject.getData());
            LoadBalancingStrategy loadBalancingStrategyEntity = LoadBalancingStrategyConverter.createJpaLoadBalancingStrategy(loadBalanceStrategy);
            loadBalanceStrategyRepository.save(loadBalancingStrategyEntity);
        } catch (InvalidProtocolBufferException e) {
            throw new SnapshotDeserializationException("Unable to deserialize load balance strategy data.", e);
        }
    }

    @Override
    public void clear() {
        loadBalanceStrategyRepository.deleteAll();
    }

    private <V> SerializedObject toSerializedObject(LoadBalanceStrategy loadBalanceStrategy) {
        return SerializedObject.newBuilder()
                               .setType(LoadBalancingStrategy.class.getName())
                               .setData(loadBalanceStrategy.toByteString())
                               .build();
    }
}
