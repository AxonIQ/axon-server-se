package io.axoniq.axonserver.enterprise.taskscheduler.task;

import io.axoniq.axonserver.enterprise.cluster.RaftGroupServiceFactory;
import io.axoniq.axonserver.enterprise.context.ContextController;
import io.axoniq.axonserver.enterprise.taskscheduler.ScheduledTask;
import io.axoniq.axonserver.enterprise.taskscheduler.TaskPublisher;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.axoniq.axonserver.RaftAdminGroup.getAdmin;

/**
 * Starts the process of deleting a node from a context. First step is to notify the admin nodes and the nodes currently
 * in the context that there is a delete starting. The admin nodes will no longer return this node a target for new
 * connections.
 * The node that is to be deleted will request a reconnect for all connected clients in that context.
 *
 * @author Marc Gathier
 * @since 4.3
 */
@Component
public class PrepareDeleteNodeFromContextTask implements ScheduledTask {

    private final Logger logger = LoggerFactory.getLogger(PrepareDeleteNodeFromContextTask.class);

    private final ContextController contextController;
    private final TaskPublisher taskPublisher;
    private final RaftGroupServiceFactory raftGroupServiceFactory;

    public PrepareDeleteNodeFromContextTask(
            RaftGroupServiceFactory raftGroupServiceFactory,
            ContextController contextController, TaskPublisher taskPublisher) {
        this.raftGroupServiceFactory = raftGroupServiceFactory;
        this.contextController = contextController;
        this.taskPublisher = taskPublisher;
    }

    @Override
    public CompletableFuture<Void> execute(Object payload) {
        NodeContext nodeContext = (NodeContext) payload;
        Collection<String> nodesInContext = contextController.getContext(nodeContext.getContext()).getNodeNames();
        Collection<String> adminNodes = contextController.getContext(getAdmin()).getNodeNames();
        Set<String> targetNodes = new HashSet<>(nodesInContext);
        targetNodes.addAll(adminNodes);
        targetNodes.forEach(n -> sendPreDeleteNodeFromContext(n, nodeContext));

        return taskPublisher.publishScheduledTask(getAdmin(), DeleteNodeFromContextTask.class.getName(),
                                                  nodeContext,
                                                  Duration.of(1, ChronoUnit.SECONDS));
    }

    private void sendPreDeleteNodeFromContext(String node, NodeContext nodeContext) {
        try {
            raftGroupServiceFactory.getRaftGroupServiceForNode(node)
                                   .prepareDeleteNodeFromContext(nodeContext.getContext(), nodeContext.getNode())
                                   .get(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.debug("Interrupted task");
            throw new MessagingPlatformException(ErrorCode.OTHER, "Prepare delete node from context interrupted");
        } catch (ExecutionException e) {
            logger.info("Prepare delete node from context failed on {}", node, e.getCause());
        } catch (TimeoutException e) {
            logger.info("Prepare delete node from context timed out on {}", node);
        }
    }

    public void prepareDeleteNodeFromContext(String name, String node, boolean preserveContext) {
        taskPublisher.publishScheduledTask(getAdmin(), PrepareDeleteNodeFromContextTask.class.getName(),
                                           new NodeContext(node, name, preserveContext),
                                           Duration.ZERO);
    }
}
