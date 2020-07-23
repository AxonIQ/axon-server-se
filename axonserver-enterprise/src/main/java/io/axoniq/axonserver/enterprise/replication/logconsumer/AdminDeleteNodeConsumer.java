package io.axoniq.axonserver.enterprise.replication.logconsumer;


import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.LogEntryConsumer;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.DeleteNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

/**
 * Deletes node from a cluster. Runs in Admin context only.
 *
 * @author Marc Gathier
 * @since 4.1
 */
@Component
public class AdminDeleteNodeConsumer implements LogEntryConsumer {

    private final Logger logger = LoggerFactory.getLogger(AdminDeleteNodeConsumer.class);

    private final ApplicationEventPublisher eventPublisher;

    public AdminDeleteNodeConsumer(ApplicationEventPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
    }

    @Override
    public String entryType() {
        return DeleteNode.class.getName();
    }

    @Override
    public void consumeLogEntry(String groupId, Entry e) throws InvalidProtocolBufferException {
        DeleteNode deleteNode = DeleteNode.parseFrom(e.getSerializedObject().getData());
        logger.warn("{}: received data: {}", groupId, deleteNode);
        eventPublisher.publishEvent(deleteNode);
    }
}
