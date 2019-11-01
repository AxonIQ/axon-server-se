package io.axoniq.axonserver.enterprise.task;

import io.axoniq.axonserver.enterprise.cluster.RaftGroupServiceFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * @author Marc Gathier
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
        DeleteNodeFromContextJob.NodeContext nodeContext = (DeleteNodeFromContextJob.NodeContext) payload;
        try {
            raftGroupServiceFactory.getRaftGroupServiceForNode(nodeContext.getNode())
                                   .deleteContext(nodeContext.getContext(), nodeContext.isPreserveEventStore()).get(1,
                                                                                                                    TimeUnit.SECONDS);
        } catch (Exception ex) {
            throw new TransientException(nodeContext.getContext() + ": DeleteContext failed", ex);
        }
    }
}
