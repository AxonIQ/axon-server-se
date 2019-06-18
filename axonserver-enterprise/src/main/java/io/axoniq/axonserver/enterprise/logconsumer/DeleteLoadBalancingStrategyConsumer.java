package io.axoniq.axonserver.enterprise.logconsumer;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.LogEntryConsumer;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.LoadBalanceStrategyController;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.LoadBalanceStrategy;
import org.springframework.stereotype.Component;

/**
 * Deletes load balancing strategies.
 *
 * @author Marc Gathier
 */
@Component
public class DeleteLoadBalancingStrategyConsumer implements LogEntryConsumer {

    public static final String DELETE_LOAD_BALANCING_STRATEGY = "DELETE_LOAD_BALANCING_STRATEGY";
    private final LoadBalanceStrategyController loadBalanceStrategyController;

    public DeleteLoadBalancingStrategyConsumer(
            LoadBalanceStrategyController loadBalanceStrategyController) {
        this.loadBalanceStrategyController = loadBalanceStrategyController;
    }

    @Override
    public String entryType() {
        return DELETE_LOAD_BALANCING_STRATEGY;
    }

    @Override
    public void consumeLogEntry(String groupId, Entry entry) throws InvalidProtocolBufferException {
        LoadBalanceStrategy strategy = LoadBalanceStrategy.parseFrom(entry.getSerializedObject().getData());
        loadBalanceStrategyController.delete(strategy.getName());
    }
}
