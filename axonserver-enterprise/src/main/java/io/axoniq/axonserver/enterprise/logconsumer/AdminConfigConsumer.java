package io.axoniq.axonserver.enterprise.logconsumer;


import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.LogEntryConsumer;
import io.axoniq.axonserver.enterprise.ContextEvents;
import io.axoniq.axonserver.enterprise.context.ContextController;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.ContextConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

/**
 * Applies Context Configuration. Runs in Admin context only.
 *
 * @author Marc Gathier
 */
@Component
public class AdminConfigConsumer implements LogEntryConsumer {

    private Logger logger = LoggerFactory.getLogger(AdminConfigConsumer.class);

    private final ContextController contextController;
    private final ApplicationEventPublisher eventPublisher;

    public AdminConfigConsumer(ContextController contextController,
                               ApplicationEventPublisher eventPublisher) {
        this.contextController = contextController;
        this.eventPublisher = eventPublisher;
    }

    @Override
    public String entryType() {
        return ContextConfiguration.class.getName();
    }

    @Override
    public void consumeLogEntry(String groupId, Entry e) throws InvalidProtocolBufferException {
        ContextConfiguration contextConfiguration = ContextConfiguration.parseFrom(e.getSerializedObject().getData());
        logger.debug("{}: received data: {}", groupId, contextConfiguration);
        contextController.updateContext(contextConfiguration);
        eventPublisher.publishEvent(new ContextEvents.ContextUpdated(groupId));
    }
}
