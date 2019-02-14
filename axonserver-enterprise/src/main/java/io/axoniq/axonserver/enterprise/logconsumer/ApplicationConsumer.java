package io.axoniq.axonserver.enterprise.logconsumer;

import io.axoniq.axonserver.access.application.JpaContextApplicationController;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.ContextApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Author: marc
 */
@Component
public class ApplicationConsumer implements LogEntryConsumer {
    private final Logger logger = LoggerFactory.getLogger(ApplicationConsumer.class);
    private final JpaContextApplicationController applicationController;

    public ApplicationConsumer(JpaContextApplicationController applicationController) {
        this.applicationController = applicationController;
    }

    @Override
    public void consumeLogEntry(String groupId, Entry entry) {
        if( entryType(entry, ContextApplication.class.getName())) {
            try {
                ContextApplication application = ContextApplication.parseFrom(entry.getSerializedObject().getData());
                applicationController.mergeApplication(application);
            } catch (Exception e) {
                logger.warn("{}: Failed to process application", groupId, e);
            }
        }

    }
}
