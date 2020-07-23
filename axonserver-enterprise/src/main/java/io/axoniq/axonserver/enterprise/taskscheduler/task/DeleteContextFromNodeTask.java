package io.axoniq.axonserver.enterprise.taskscheduler.task;

import io.axoniq.axonserver.enterprise.replication.group.RaftGroupServiceFactory;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.taskscheduler.ScheduledTask;
import io.axoniq.axonserver.taskscheduler.TransientException;
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
    public CompletableFuture<Void> executeAsync(String context, Object payload) {
        NodeContext nodeContext = (NodeContext) payload;
        try {
            return raftGroupServiceFactory.getRaftGroupServiceForNode(nodeContext.getNode())
                                          .deleteReplicationGroup(nodeContext.getContext(),
                                                              nodeContext.isPreserveEventStore())
                                          .exceptionally(t -> {
                                              throw new TransientException(t.getMessage(), t);
                                          });
        } catch (MessagingPlatformException mpe) {
            if (ErrorCode.NO_SUCH_NODE.equals(mpe.getErrorCode())) {
                // ignore
            }
            throw mpe;
        }
    }
}
