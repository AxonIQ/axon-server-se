package io.axoniq.axonserver.enterprise.logconsumer;

import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.Application;
import io.axoniq.platform.application.ApplicationController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Author: marc
 */
@Component
public class DeleteApplicationConsumer implements LogEntryConsumer {
    public static final String DELETE_APPLICATION = "DELETE_APPLICATION";
    private final Logger logger = LoggerFactory.getLogger(DeleteApplicationConsumer.class);
    private final ApplicationController applicationController;

    public DeleteApplicationConsumer(ApplicationController applicationController) {
        this.applicationController = applicationController;
    }

    @Override
    public void consumeLogEntry(String groupId, Entry entry) {
        if( entryType(entry, DELETE_APPLICATION)) {
            try {
                Application application = Application.parseFrom(entry.getSerializedObject().getData());
                logger.warn("{}: Delete application: {}", groupId, application.getName());
                applicationController.delete(application.getName());
            } catch (Exception e) {
                logger.warn("{}: Failed to process application", groupId, e);
            }
        }

    }
}
