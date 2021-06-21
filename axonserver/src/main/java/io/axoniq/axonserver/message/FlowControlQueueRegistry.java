package io.axoniq.axonserver.message;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.message.command.WrappedCommand;
import io.axoniq.axonserver.message.query.WrappedQuery;
import io.axoniq.axonserver.metric.BaseMetricName;
import io.axoniq.axonserver.metric.MeterFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Comparator;

@Component
public class FlowControlQueueRegistry {

    private final FlowControlQueues<WrappedCommand> commandQueues;
    private final FlowControlQueues<WrappedQuery> queryQueues;

    public FlowControlQueueRegistry(
            @Value("${axoniq.axonserver.command-queue-capacity-per-client:10000}") int commandQueueCapacity,
            @Value("${axoniq.axonserver.query-queue-capacity-per-client:10000}") int queryQueueCapacity,
            MeterFactory meterFactory
            ) {
        commandQueues = new FlowControlQueues<>(Comparator
                                                                                     .comparing(WrappedCommand::priority).reversed(),
                                                                             commandQueueCapacity,
                                                                             BaseMetricName.AXON_APPLICATION_COMMAND_QUEUE_SIZE,
                                                                             meterFactory,
                                                                             ErrorCode.COMMAND_DISPATCH_ERROR);
        queryQueues = new FlowControlQueues<>(
                Comparator
                        .comparing(WrappedQuery::priority).reversed(),
                queryQueueCapacity,
                BaseMetricName.AXON_APPLICATION_QUERY_QUEUE_SIZE,
                meterFactory,
                ErrorCode.QUERY_DISPATCH_ERROR);

    }

    public FlowControlQueues<WrappedCommand> commandQueues() {
        return commandQueues;
    }

    public FlowControlQueues<WrappedQuery> queryQueues() {
        return queryQueues;
    }
}
