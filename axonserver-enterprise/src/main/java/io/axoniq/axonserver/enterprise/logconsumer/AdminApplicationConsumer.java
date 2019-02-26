package io.axoniq.axonserver.enterprise.logconsumer;

import io.axoniq.axonserver.access.application.AppEvents;
import io.axoniq.axonserver.access.application.ApplicationController;
import io.axoniq.axonserver.grpc.ApplicationProtoConverter;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.Application;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import static io.axoniq.axonserver.RaftAdminGroup.isAdmin;

/**
 * @author Marc Gathier
 *
 * Applies LogEntries with Application
 * Only run in Admin context
 */
@Component
public class AdminApplicationConsumer implements LogEntryConsumer {
    private final ApplicationController applicationController;
    private final ApplicationEventPublisher applicationEventPublisher;
    private final Logger logger = LoggerFactory.getLogger(AdminApplicationConsumer.class);

    public AdminApplicationConsumer(ApplicationController applicationController, ApplicationEventPublisher applicationEventPublisher) {
        this.applicationController = applicationController;
        this.applicationEventPublisher = applicationEventPublisher;
    }

    @Override
    public void consumeLogEntry(String groupId, Entry e) {
        if( ! isAdmin(groupId)) return;
        if( entryType(e, Application.class.getName())) {
            Application application = null;
            try {
                application = Application.parseFrom(e.getSerializedObject().getData());
                applicationController.synchronize(ApplicationProtoConverter.createJpaApplication(application));
                applicationEventPublisher.publishEvent(new AppEvents.AppUpdated(application.getName()));
            } catch (Exception e1) {
                logger.warn("Failed to update application: {}", application, e1);
            }
        }

    }


}
