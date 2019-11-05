package io.axoniq.axonserver.enterprise.task.job;

import io.axoniq.axonserver.enterprise.cluster.RaftGroupServiceFactory;
import io.axoniq.axonserver.enterprise.task.ScheduledJob;
import io.axoniq.axonserver.enterprise.task.TransientException;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Job that removes a context from a node, potentially keeping the event data. This is called after the context is updated with
 * the new configuration without the specified node.
 * @author Marc Gathier
 * @since 4.3
 */
@Component
public class DeleteContextFromNodeJob implements ScheduledJob {

    private final RaftGroupServiceFactory raftGroupServiceFactory;

    public DeleteContextFromNodeJob(
            RaftGroupServiceFactory raftGroupServiceFactory) {
        this.raftGroupServiceFactory = raftGroupServiceFactory;
    }

    @Override
    public void execute(Object payload) {
        NodeContext nodeContext = (NodeContext) payload;
        try {
            raftGroupServiceFactory.getRaftGroupServiceForNode(nodeContext.getNode())
                                   .deleteContext(nodeContext.getContext(), nodeContext.isPreserveEventStore()).get(1,
                                                                                                                    TimeUnit.SECONDS);
        } catch (Exception ex) {
            throw new TransientException(nodeContext.getContext() + ": DeleteContext failed", ex);
        }
    }
}
