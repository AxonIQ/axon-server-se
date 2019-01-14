package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.enterprise.cluster.events.ApplicationSynchronizationEvents;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.grpc.ApplicationProtoConverter;
import io.axoniq.platform.application.ApplicationController;
import io.axoniq.platform.application.ApplicationModelController;
import io.axoniq.platform.application.jpa.Application;
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
    private final ApplicationModelController applicationModelController;

    public ApplicationSynchronizer(ApplicationController applicationController,
                                   ApplicationModelController applicationModelController) {
        this.applicationController = applicationController;
        this.applicationModelController = applicationModelController;
    }

    @EventListener
    public void on(ApplicationSynchronizationEvents.ApplicationReceived event) {
        io.axoniq.axonserver.grpc.internal.Application application = event.getApplication();
        try {
            switch (application.getAction()) {
                case MERGE:
                    applicationController.synchronize(ApplicationProtoConverter
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
            if( applicationModelController.getModelVersion(Application.class) < event.getApplications().getVersion()) {
                applicationController.clearApplications();
                event.getApplications().getApplicationList().forEach(app -> applicationController
                        .synchronize(ApplicationProtoConverter.createJpaApplication(app)));
                applicationModelController.updateModelVersion(Application.class, event.getApplications().getVersion());
            }
        }
    }

    @EventListener
    public void on(ClusterEvents.AxonServerInstanceConnected event) {
        if (applicationModelController.getModelVersion(Application.class) < event.getModelVersion(Application.class.getName())) {
            event.getRemoteConnection().requestApplications();
        }
    }
}
