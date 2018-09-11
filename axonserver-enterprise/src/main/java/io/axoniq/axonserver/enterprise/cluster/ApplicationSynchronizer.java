package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.enterprise.cluster.events.ApplicationSynchronizationEvents;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.grpc.ProtoConverter;
import io.axoniq.platform.application.ApplicationController;
import io.axoniq.platform.grpc.Application;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Controller;

/**
 * Author: marc
 */
@Controller
public class ApplicationSynchronizer {
    private final Logger logger = LoggerFactory.getLogger(ApplicationSynchronizer.class);
    private final ApplicationController applicationController;

    public ApplicationSynchronizer(ApplicationController applicationController) {
        this.applicationController = applicationController;
    }

    @EventListener
    public void on(ApplicationSynchronizationEvents.ApplicationReceived event) {
        Application application = event.getApplication();
        try {
            switch (application.getAction()) {
                case MERGE:
                    applicationController.synchronize(ProtoConverter
                                                              .createJpaApplication(application));
                    break;
                case DELETE:
                    applicationController.delete(application.getName());
                    break;
                default:
                    break;
            }
        } catch (Exception ex) {
            logger.debug("Failed to update application: {} - {}", application, ex.getMessage());
        }
    }

    @EventListener
    public void on(ApplicationSynchronizationEvents.ApplicationsReceived event) {
        synchronized (applicationController) {
            if( applicationController.getModelVersion() < event.getApplications().getVersion()) {
                applicationController.clearApplications();
                event.getApplications().getApplicationList().forEach(app -> applicationController
                        .synchronize(ProtoConverter.createJpaApplication(app)));
                applicationController.updateModelVersion(event.getApplications().getVersion());
            }
        }
    }

    @EventListener
    public void on(ClusterEvents.AxonHubInstanceConnected event) {
        if (applicationController.getModelVersion() < event.getModelVersion()) {
            event.getRemoteConnection().requestApplications();
        }
    }
}
