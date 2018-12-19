package io.axoniq.axonserver.enterprise.logconsumer;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.component.processor.balancing.jpa.ProcessorLoadBalancingController;
import io.axoniq.axonserver.grpc.ProtoConverter;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;
import org.springframework.stereotype.Component;

/**
 * Author: marc
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
                processorLoadBalancingController.save(ProtoConverter.createJpaProcessorLoadBalancing(processorLBStrategy));
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
        }
    }
}
