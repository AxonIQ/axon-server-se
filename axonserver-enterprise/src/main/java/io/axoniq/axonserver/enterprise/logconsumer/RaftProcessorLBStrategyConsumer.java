package io.axoniq.axonserver.enterprise.logconsumer;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.LogEntryConsumer;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.RaftProcessorLoadBalancing;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.RaftProcessorLoadBalancingService;
import io.axoniq.axonserver.grpc.ProcessorLBStrategyConverter;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;
import org.springframework.stereotype.Component;

/**
 * Applies Processor Load Balancing Strategy log entry.
 *
 * @author Marc Gathier
 * @since 4.2
 */
@Component
public class RaftProcessorLBStrategyConsumer implements LogEntryConsumer {

    private final RaftProcessorLoadBalancingService processorLoadBalancingService;

    public RaftProcessorLBStrategyConsumer(
            RaftProcessorLoadBalancingService processorLoadBalancingService) {
        this.processorLoadBalancingService = processorLoadBalancingService;
    }

    @Override
    public String entryType() {
        return RaftProcessorLoadBalancing.class.getName();
    }

    @Override
    public void consumeLogEntry(String groupId, Entry entry) throws InvalidProtocolBufferException {
        ProcessorLBStrategy processorLBStrategy = ProcessorLBStrategy.parseFrom(entry.getSerializedObject().getData());
        processorLoadBalancingService.save(ProcessorLBStrategyConverter
                                                   .createJpaRaftProcessorLoadBalancing(processorLBStrategy));
    }
}
