package io.axoniq.axonserver.enterprise.replication.logconsumer;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.LogEntryConsumer;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.ReplicationGroupProcessorLoadBalancing;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.ReplicationGroupProcessorLoadBalancingService;
import io.axoniq.axonserver.grpc.ProcessorLBStrategyConverter;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;
import org.springframework.stereotype.Component;

/**
 * Applies Processor Load Balancing Strategy log entry at replication group level.
 *
 * @author Marc Gathier
 * @since 4.2
 */
@Component
public class ProcessorLBStrategyConsumer implements LogEntryConsumer {

    private final ReplicationGroupProcessorLoadBalancingService processorLoadBalancingService;

    public ProcessorLBStrategyConsumer(
            ReplicationGroupProcessorLoadBalancingService processorLoadBalancingService) {
        this.processorLoadBalancingService = processorLoadBalancingService;
    }

    @Override
    public String entryType() {
        return ReplicationGroupProcessorLoadBalancing.class.getName();
    }

    @Override
    public void consumeLogEntry(String groupId, Entry entry) throws InvalidProtocolBufferException {
        ProcessorLBStrategy processorLBStrategy = ProcessorLBStrategy.parseFrom(entry.getSerializedObject().getData());
        processorLoadBalancingService.save(ProcessorLBStrategyConverter
                                                   .createJpaRaftProcessorLoadBalancing(processorLBStrategy));
    }
}
