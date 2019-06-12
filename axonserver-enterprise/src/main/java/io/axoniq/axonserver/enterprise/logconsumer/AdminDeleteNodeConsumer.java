package io.axoniq.axonserver.enterprise.logconsumer;


import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.LogEntryConsumer;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.DeleteNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

/**
 * @author Marc Gathier
 */
@Component
public class AdminDeleteNodeConsumer implements LogEntryConsumer {

    private Logger logger = LoggerFactory.getLogger(AdminDeleteNodeConsumer.class);

    private final ApplicationEventPublisher eventPublisher;

    public AdminDeleteNodeConsumer(ClusterController clusterController,
                                   ApplicationEventPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
    }

    @Override
    public void consumeLogEntry(String groupId, Entry e) throws InvalidProtocolBufferException {
        // TODO: 6/12/2019 should we check for the group here? shouldn't apply entry be propagated to correct group members already?
        if ( /*isAdmin(groupId) && */entryType(e, DeleteNode.class)) {
            DeleteNode deleteNode = DeleteNode.parseFrom(e.getSerializedObject().getData());
            logger.warn("{}: received data: {}", groupId, deleteNode);
            eventPublisher.publishEvent(deleteNode);
        }
    }
}
