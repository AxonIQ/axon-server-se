package io.axoniq.axonserver.enterprise.task.job;

import io.axoniq.axonserver.enterprise.cluster.RaftConfigServiceFactory;
import io.axoniq.axonserver.enterprise.task.ScheduledJob;
import io.axoniq.axonserver.enterprise.task.TaskPublisher;
import org.springframework.stereotype.Component;

/**
 * Job that deletes removes a node from a context. Sends a request to the leader of the context.
 * The leader will send the updated configuration to all other (remaining) nodes.
 * The node that is deleted from the context is not notified here.
 * @author Marc Gathier
 * @since 4.3
 */
@Component
public class DeleteNodeFromContextJob implements ScheduledJob {

    private final RaftConfigServiceFactory raftConfigServiceFactory;
    private final TaskPublisher taskPublisher;

    public DeleteNodeFromContextJob(
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
    public void execute(Object payload)  {
        NodeContext nodeContext = (NodeContext) payload;
        raftConfigServiceFactory.getRaftConfigService().deleteNodeFromContext(nodeContext.getContext(),
                                                                              nodeContext.getNode());
        taskPublisher.publishTask(DeleteContextFromNodeJob.class.getName(), nodeContext, 100);
    }
}
