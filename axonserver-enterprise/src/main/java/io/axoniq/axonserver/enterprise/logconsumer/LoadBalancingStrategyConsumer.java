package io.axoniq.axonserver.enterprise.logconsumer;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.component.processor.balancing.jpa.LoadBalanceStrategyController;
import io.axoniq.axonserver.grpc.ProtoConverter;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.LoadBalanceStrategy;
import org.springframework.stereotype.Component;

/**
 * Author: marc
 */
@Component
public class LoadBalancingStrategyConsumer implements LogEntryConsumer {
    private final LoadBalanceStrategyController loadBalanceStrategyController;

    public LoadBalancingStrategyConsumer(
            LoadBalanceStrategyController loadBalanceStrategyController) {
        this.loadBalanceStrategyController = loadBalanceStrategyController;
    }

    @Override
    public void consumeLogEntry(String groupId, Entry entry) {
        if( entryType(entry, LoadBalanceStrategy.class.getName())) {
            try {
                LoadBalanceStrategy strategy = LoadBalanceStrategy.parseFrom(entry.getSerializedObject().getData());
                loadBalanceStrategyController.save(ProtoConverter.createJpaLoadBalancingStrategy(strategy));
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
        }
    }

}
