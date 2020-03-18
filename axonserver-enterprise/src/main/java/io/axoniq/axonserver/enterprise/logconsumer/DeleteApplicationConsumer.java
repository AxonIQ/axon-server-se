package io.axoniq.axonserver.enterprise.logconsumer;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.access.application.AppEvents;
import io.axoniq.axonserver.access.application.ApplicationController;
import io.axoniq.axonserver.cluster.LogEntryConsumer;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.Application;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

/**
 * Deletes applications.
 *
 * @author Marc Gathier
 */
@Component
public class DeleteApplicationConsumer implements LogEntryConsumer {

    public static final String DELETE_APPLICATION = "DELETE_APPLICATION";
    private final Logger logger = LoggerFactory.getLogger(DeleteApplicationConsumer.class);
    private final ApplicationController applicationController;
    private final ApplicationEventPublisher applicationEventPublisher;

    public DeleteApplicationConsumer(ApplicationController applicationController,
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