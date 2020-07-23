package io.axoniq.axonserver.enterprise.taskscheduler.task;

import io.axoniq.axonserver.enterprise.cluster.ClusterPublisher;
import io.axoniq.axonserver.enterprise.replication.admin.RaftConfigServiceFactory;
import io.axoniq.axonserver.taskscheduler.ScheduledTask;
import io.axoniq.axonserver.grpc.internal.ConnectorCommand;
import io.axoniq.axonserver.grpc.internal.DeleteNode;
import org.springframework.stereotype.Component;

/**
 * @author Marc Gathier
 */
@Component
public class UnregisterNodeTask implements ScheduledTask {

    private final RaftConfigServiceFactory raftGroupServiceFactory;
    private final ClusterPublisher clusterPublisher;

    public UnregisterNodeTask(RaftConfigServiceFactory raftGroupServiceFactory,
                              ClusterPublisher clusterPublisher) {
        this.raftGroupServiceFactory = raftGroupServiceFactory;
        this.clusterPublisher = clusterPublisher;
    }

    @Override
    public void execute(String context, Object payload) {
        String node = (String) payload;
        raftGroupServiceFactory.getRaftConfigService()
                               .deleteNodeIfEmpty(node);
        clusterPublisher.publish(ConnectorCommand.newBuilder()
                                                 .setDeleteNode(DeleteNode.newBuilder()
                                                                          .setNodeName(node)
                                                                          .build())
                                                 .build());
    }
}
