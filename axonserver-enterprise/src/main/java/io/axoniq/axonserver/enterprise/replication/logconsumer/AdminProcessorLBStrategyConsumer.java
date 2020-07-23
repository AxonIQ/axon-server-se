package io.axoniq.axonserver.enterprise.replication.logconsumer;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.LogEntryConsumer;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.AdminProcessorLoadBalancingService;
import io.axoniq.axonserver.grpc.ProcessorLBStrategyConverter;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;
import org.springframework.stereotype.Component;

/**
 * Applies Processor Load Balancing Strategy log entry in the _admin context.
 *
 * @author Marc Gathier
 * @since 4.1
 */
@Component
public class AdminProcessorLBStrategyConsumer implements LogEntryConsumer {

    private final AdminProcessorLoadBalancingService processorLoadBalancingService;

    public AdminProcessorLBStrategyConsumer(
            AdminProcessorLoadBalancingService processorLoadBalancingService) {
        this.processorLoadBalancingService = processorLoadBalancingService;
    }

    @Override
    public String entryType() {
        return ProcessorLBStrategy.class.getName();
    }

    @Override
    public void consumeLogEntry(String groupId, Entry entry) throws InvalidProtocolBufferException {
        ProcessorLBStrategy processorLBStrategy = ProcessorLBStrategy.parseFrom(entry.getSerializedObject().getData());
        processorLoadBalancingService.save(ProcessorLBStrategyConverter
                                                   .createJpaProcessorLoadBalancing(processorLBStrategy));
    }
}
