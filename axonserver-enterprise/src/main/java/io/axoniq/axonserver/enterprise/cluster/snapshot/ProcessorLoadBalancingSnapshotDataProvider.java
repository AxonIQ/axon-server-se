package io.axoniq.axonserver.enterprise.cluster.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.snapshot.SnapshotDeserializationException;
import io.axoniq.axonserver.component.processor.balancing.jpa.ProcessorLoadBalancing;
import io.axoniq.axonserver.component.processor.balancing.jpa.ProcessorLoadBalancingRepository;
import io.axoniq.axonserver.grpc.ProtoConverter;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;
import reactor.core.publisher.Flux;

import static io.axoniq.axonserver.grpc.ProtoConverter.createJpaProcessorLoadBalancing;

/**
 * @author Milan Savic
 */
public class ProcessorLoadBalancingSnapshotDataProvider implements SnapshotDataProvider {

    private final String context;
    private final ProcessorLoadBalancingRepository processorLoadBalancingRepository;

    public ProcessorLoadBalancingSnapshotDataProvider(
            String context, ProcessorLoadBalancingRepository processorLoadBalancingRepository) {
        this.context = context;
        this.processorLoadBalancingRepository = processorLoadBalancingRepository;
    }

    @Override
    public int order() {
        return 30;
    }

    @Override
    public Flux<SerializedObject> provide(long from, long to) {
        return Flux.fromIterable(processorLoadBalancingRepository.findByContext(context))
                   .map(ProtoConverter::createProcessorLBStrategy)
                   .map(this::toSerializedObject);
    }

    @Override
    public boolean canConsume(String type) {
        return ProcessorLoadBalancing.class.getName().equals(type);
    }

    @Override
    public void consume(SerializedObject serializedObject) {
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
        processorLoadBalancingRepository.deleteAllByProcessorContext(context);
    }

    private SerializedObject toSerializedObject(ProcessorLBStrategy processorLBStrategy) {
        return SerializedObject.newBuilder()
                               .setType(ProcessorLoadBalancing.class.getName())
                               .setData(processorLBStrategy.toByteString())
                               .build();
    }
}
