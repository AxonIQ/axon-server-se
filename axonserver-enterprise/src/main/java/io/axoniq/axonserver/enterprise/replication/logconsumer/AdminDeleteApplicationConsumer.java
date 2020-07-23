package io.axoniq.axonserver.enterprise.replication.logconsumer;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.access.application.AppEvents;
import io.axoniq.axonserver.access.application.AdminApplicationController;
import io.axoniq.axonserver.cluster.LogEntryConsumer;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.Application;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

/**
 * Applies a DELETE_APPLICATION log entry in the _admin replication group. Deletes the application from the
 * tables used by the Axon Server console.
 *
 * @author Marc Gathier
 * @since 4.1
 */
@Component
public class AdminDeleteApplicationConsumer implements LogEntryConsumer {

    public static final String DELETE_APPLICATION = "DELETE_APPLICATION";
    private final Logger logger = LoggerFactory.getLogger(AdminDeleteApplicationConsumer.class);
    private final AdminApplicationController applicationController;
    private final ApplicationEventPublisher applicationEventPublisher;

    public AdminDeleteApplicationConsumer(AdminApplicationController applicationController,
                                          ApplicationEventPublisher applicationEventPublisher) {
        this.applicationController = applicationController;
        this.applicationEventPublisher = applicationEventPublisher;
    }

    @Override
    public String entryType() {
        return DELETE_APPLICATION;
    }

    @Override
    public void consumeLogEntry(String groupId, Entry entry) throws InvalidProtocolBufferException {
        Application application = Application.parseFrom(entry.getSerializedObject().getData());
        logger.debug("{}: Delete application: {}", groupId, application.getName());
        applicationController.delete(application.getName());
        applicationEventPublisher.publishEvent(new AppEvents.AppDeleted(application.getName()));
    }
}
