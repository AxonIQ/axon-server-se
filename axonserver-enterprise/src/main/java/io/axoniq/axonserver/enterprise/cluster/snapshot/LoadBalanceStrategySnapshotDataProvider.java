package io.axoniq.axonserver.enterprise.cluster.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.snapshot.SnapshotDeserializationException;
import io.axoniq.axonserver.component.processor.balancing.jpa.LoadBalanceStrategyRepository;
import io.axoniq.axonserver.component.processor.balancing.jpa.LoadBalancingStrategy;
import io.axoniq.axonserver.grpc.ProtoConverter;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import io.axoniq.axonserver.grpc.internal.LoadBalanceStrategy;
import reactor.core.publisher.Flux;

import static io.axoniq.axonserver.grpc.ProtoConverter.createJpaLoadBalancingStrategy;

/**
 * @author Milan Savic
 */
public class LoadBalanceStrategySnapshotDataProvider implements SnapshotDataProvider {

    private final LoadBalanceStrategyRepository loadBalanceStrategyRepository;

    public LoadBalanceStrategySnapshotDataProvider(LoadBalanceStrategyRepository loadBalanceStrategyRepository) {
        this.loadBalanceStrategyRepository = loadBalanceStrategyRepository;
    }

    @Override
    public Flux<SerializedObject> provide(long from, long to) {
        return Flux.fromIterable(loadBalanceStrategyRepository.findAll())
                   .map(ProtoConverter::createLoadBalanceStrategy)
                   .map(this::toSerializedObject);
    }

    @Override
    public int order() {
        return 20;
    }

    @Override
    public boolean canConsume(String type) {
        return LoadBalancingStrategy.class.getName().equals(type);
    }

    @Override
    public void consume(SerializedObject serializedObject) {
        try {
            LoadBalanceStrategy loadBalanceStrategy = LoadBalanceStrategy.parseFrom(serializedObject.getData());
            LoadBalancingStrategy loadBalancingStrategyEntity = createJpaLoadBalancingStrategy(loadBalanceStrategy);
            loadBalanceStrategyRepository.save(loadBalancingStrategyEntity);
        } catch (InvalidProtocolBufferException e) {
            throw new SnapshotDeserializationException("Unable to deserialize load balance strategy data.", e);
        }
    }

    private <V> SerializedObject toSerializedObject(LoadBalanceStrategy loadBalanceStrategy) {
        return SerializedObject.newBuilder()
                               .setType(LoadBalancingStrategy.class.getName())
                               .setData(loadBalanceStrategy.toByteString())
                               .build();
    }
}
