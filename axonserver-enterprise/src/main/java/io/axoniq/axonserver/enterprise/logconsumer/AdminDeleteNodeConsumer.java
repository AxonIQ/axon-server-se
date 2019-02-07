package io.axoniq.axonserver.enterprise.logconsumer;


import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.GrpcRaftController;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.DeleteNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

/**
 * Author: marc
 */
@Component
public class AdminDeleteNodeConsumer implements LogEntryConsumer {

    private Logger logger = LoggerFactory.getLogger(AdminDeleteNodeConsumer.class);

    private final ApplicationEventPublisher eventPublisher;
    private final ClusterController clusterController;

    public AdminDeleteNodeConsumer(ClusterController clusterController,
                                   ApplicationEventPublisher eventPublisher) {
        this.clusterController = clusterController;
        this.eventPublisher = eventPublisher;
    }

    @Override
    public void consumeLogEntry(String groupId, Entry e) {
        if( groupId.equals(GrpcRaftController.ADMIN_GROUP) && entryType(e, DeleteNode.class)) {
                try {
                    DeleteNode deleteNode = DeleteNode.parseFrom(e.getSerializedObject().getData());
                    logger.warn("{}: received data: {}", groupId, deleteNode);
                    clusterController.deleteNode(deleteNode.getNodeName());
                    eventPublisher.publishEvent(new ClusterEvents.AxonServerNodeDeleted(deleteNode.getNodeName()));
                } catch (Exception e1) {
                    logger.warn("{}: Failed to process log entry: {}", groupId, e, e1);
                }
        }
    }
}
