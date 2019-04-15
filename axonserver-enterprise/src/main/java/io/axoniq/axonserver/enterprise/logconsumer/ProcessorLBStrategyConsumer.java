package io.axoniq.axonserver.enterprise.logconsumer;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.ProcessorLoadBalancingController;
import io.axoniq.axonserver.grpc.ProcessorLBStrategyConverter;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;
import org.springframework.stereotype.Component;

/**
 * @author Marc Gathier
 */
@Component
public class ProcessorLBStrategyConsumer implements LogEntryConsumer {

    private final ProcessorLoadBalancingController processorLoadBalancingController;

    public ProcessorLBStrategyConsumer(
            ProcessorLoadBalancingController processorLoadBalancingController) {
        this.processorLoadBalancingController = processorLoadBalancingController;
    }

    @Override
    public void consumeLogEntry(String groupId, Entry entry) {
        if( entryType(entry, ProcessorLBStrategy.class)) {
            try {
                ProcessorLBStrategy processorLBStrategy = ProcessorLBStrategy.parseFrom(entry.getSerializedObject().getData());
                processorLoadBalancingController.save(ProcessorLBStrategyConverter
                                                              .createJpaProcessorLoadBalancing(processorLBStrategy));
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
        }
    }
}
