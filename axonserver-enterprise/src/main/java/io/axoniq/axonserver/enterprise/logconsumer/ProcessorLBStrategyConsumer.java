package io.axoniq.axonserver.enterprise.logconsumer;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.ProcessorLoadBalancingService;
import io.axoniq.axonserver.grpc.ProcessorLBStrategyConverter;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;
import org.springframework.stereotype.Component;

/**
 * @author Marc Gathier
 */
@Component
public class ProcessorLBStrategyConsumer implements LogEntryConsumer {

    private final ProcessorLoadBalancingService processorLoadBalancingService;

    public ProcessorLBStrategyConsumer(
            ProcessorLoadBalancingService processorLoadBalancingService) {
        this.processorLoadBalancingService = processorLoadBalancingService;
    }

    @Override
    public void consumeLogEntry(String groupId, Entry entry) throws InvalidProtocolBufferException {
        if (entryType(entry, ProcessorLBStrategy.class)) {
            ProcessorLBStrategy processorLBStrategy = ProcessorLBStrategy.parseFrom(entry.getSerializedObject()
                                                                                         .getData());
            processorLoadBalancingService.save(ProcessorLBStrategyConverter
                                                       .createJpaProcessorLoadBalancing(processorLBStrategy));
        }
    }
}
