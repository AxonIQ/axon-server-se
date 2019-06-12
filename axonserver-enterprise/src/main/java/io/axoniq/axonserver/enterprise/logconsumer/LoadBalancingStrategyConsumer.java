package io.axoniq.axonserver.enterprise.logconsumer;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.LogEntryConsumer;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.LoadBalanceStrategyController;
import io.axoniq.axonserver.grpc.LoadBalancingStrategyConverter;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.LoadBalanceStrategy;
import org.springframework.stereotype.Component;

/**
 * @author Marc Gathier
 */
@Component
public class LoadBalancingStrategyConsumer implements LogEntryConsumer {

    private final LoadBalanceStrategyController loadBalanceStrategyController;

    public LoadBalancingStrategyConsumer(
            LoadBalanceStrategyController loadBalanceStrategyController) {
        this.loadBalanceStrategyController = loadBalanceStrategyController;
    }

    @Override
    public void consumeLogEntry(String groupId, Entry entry) throws InvalidProtocolBufferException {
        if (entryType(entry, LoadBalanceStrategy.class.getName())) {
            LoadBalanceStrategy strategy = LoadBalanceStrategy.parseFrom(entry.getSerializedObject().getData());
            loadBalanceStrategyController.save(LoadBalancingStrategyConverter
                                                       .createJpaLoadBalancingStrategy(strategy));
        }
    }
}
