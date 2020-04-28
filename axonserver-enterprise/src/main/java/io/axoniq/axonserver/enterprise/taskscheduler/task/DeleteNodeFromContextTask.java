package io.axoniq.axonserver.enterprise.taskscheduler.task;

import io.axoniq.axonserver.enterprise.cluster.RaftConfigServiceFactory;
import io.axoniq.axonserver.taskscheduler.ScheduledTask;
import io.axoniq.axonserver.enterprise.taskscheduler.TaskPublisher;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletableFuture;

import static io.axoniq.axonserver.RaftAdminGroup.getAdmin;

/**
 * Task that deletes removes a node from a context. Sends a request to the leader of the context.
 * The leader will send the updated configuration to all other (remaining) nodes.
 * The node that is deleted from the context is not notified here.
 * @author Marc Gathier
 * @since 4.3
 */
@Component
public class DeleteNodeFromContextTask implements ScheduledTask {

    private final RaftConfigServiceFactory raftConfigServiceFactory;
    private final TaskPublisher taskPublisher;

    public DeleteNodeFromContextTask(
            RaftConfigServiceFactory raftConfigServiceFactory,
            TaskPublisher taskPublisher) {
        this.raftConfigServiceFactory = raftConfigServiceFactory;
        this.taskPublisher = taskPublisher;
    }

    /**
     * Sends the request to the leader of the context to remove a node.
     * When completed it publishes a new task to delete the context on the node that was removed.
     *
     * @param payload the {@link NodeContext} information
     */
    @Override
    public CompletableFuture<Void> executeAsync(String context, Object payload) {
        NodeContext nodeContext = (NodeContext) payload;
        raftConfigServiceFactory.getRaftConfigService().deleteNodeFromContext(nodeContext.getContext(),
                                                                              nodeContext.getNode());
        return taskPublisher.publishScheduledTask(getAdmin(), DeleteContextFromNodeTask.class.getName(),
                                                  nodeContext,
                                                  Duration.of(100, ChronoUnit.MILLIS))
                            .thenApply(taskId -> null);
    }
}
