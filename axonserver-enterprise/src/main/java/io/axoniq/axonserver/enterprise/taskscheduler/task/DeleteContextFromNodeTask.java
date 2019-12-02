package io.axoniq.axonserver.enterprise.taskscheduler.task;

import io.axoniq.axonserver.enterprise.cluster.RaftGroupServiceFactory;
import io.axoniq.axonserver.enterprise.taskscheduler.ScheduledTask;
import io.axoniq.axonserver.enterprise.taskscheduler.TransientException;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * Task that removes a context from a node, potentially keeping the event data. This is called after the context is updated with
 * the new configuration without the specified node.
 * @author Marc Gathier
 * @since 4.3
 */
@Component
public class DeleteContextFromNodeTask implements ScheduledTask {

    private final RaftGroupServiceFactory raftGroupServiceFactory;

    public DeleteContextFromNodeTask(
            RaftGroupServiceFactory raftGroupServiceFactory) {
        this.raftGroupServiceFactory = raftGroupServiceFactory;
    }

    @Override
    public CompletableFuture<Void> execute(Object payload) {
        NodeContext nodeContext = (NodeContext) payload;
        return raftGroupServiceFactory.getRaftGroupServiceForNode(nodeContext.getNode())
                                      .deleteContext(nodeContext.getContext(), nodeContext.isPreserveEventStore())
                                      .exceptionally(t -> {
                                          throw new TransientException(t.getMessage(), t);
                                      });
    }
}
