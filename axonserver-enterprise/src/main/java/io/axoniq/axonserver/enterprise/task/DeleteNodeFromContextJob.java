package io.axoniq.axonserver.enterprise.task;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.enterprise.cluster.RaftConfigServiceFactory;
import org.springframework.stereotype.Component;

/**
 * @author Marc Gathier
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

    @Override
    public void execute(Object payload) throws JsonProcessingException {
        NodeContext nodeContext = (NodeContext) payload;
        raftConfigServiceFactory.getRaftConfigService().deleteNodeFromContext(nodeContext.getContext(),
                                                                              nodeContext.getNode());
        taskPublisher.publishTask(DeleteContextFromNodeJob.class.getName(), nodeContext, 100);
    }

    public void deleteNodeFromContext(String name, String node, boolean preserveContext)
            throws JsonProcessingException {
        taskPublisher.publishTask(DeleteNodeFromContextJob.class.getName(),
                                  new NodeContext(node, name, preserveContext),
                                  100);
    }

    @KeepNames
    public static class NodeContext {

        private String node;
        private String context;
        private boolean preserveEventStore;

        public NodeContext() {
        }

        public NodeContext(String node, String context, boolean preserveEventStore) {
            this.node = node;
            this.context = context;
            this.preserveEventStore = preserveEventStore;
        }

        public String getNode() {
            return node;
        }

        public void setNode(String node) {
            this.node = node;
        }

        public String getContext() {
            return context;
        }

        public void setContext(String context) {
            this.context = context;
        }

        public boolean isPreserveEventStore() {
            return preserveEventStore;
        }

        public void setPreserveEventStore(boolean preserveEventStore) {
            this.preserveEventStore = preserveEventStore;
        }
    }
}
