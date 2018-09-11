package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.cluster.events.UserSynchronizationEvents;
import io.axoniq.axonserver.grpc.ProtoConverter;
import io.axoniq.platform.application.ApplicationController;
import io.axoniq.platform.user.UserController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Controller;

/**
 * Author: marc
 */
@Controller
public class UserSynchronizer {
    private final Logger logger = LoggerFactory.getLogger(UserSynchronizer.class);
    private final UserController userController;
    private final ApplicationController applicationController;

    public UserSynchronizer(UserController userController, ApplicationController applicationController) {
        this.userController = userController;
        this.applicationController = applicationController;
    }


    @EventListener
    public void on(UserSynchronizationEvents.UserReceived event) {
        try {
            switch (event.getUser().getAction()) {
                case MERGE:
                    userController.syncUser(ProtoConverter.createJpaUser(event.getUser()));
                    break;
                case DELETE:
                    userController.deleteUser(event.getUser().getName());
                    break;
                case UNRECOGNIZED:
                    break;
            }
        } catch (Exception ex) {
            logger.debug("Failed to update user: {} - {}", event.getUser(), ex.getMessage());
        }
    }

    @EventListener
    public void on(UserSynchronizationEvents.UsersReceived event) {
        synchronized (userController) {
            if( applicationController.getModelVersion() < event.getUsers().getVersion()) {
                userController.clearUsers();
                event.getUsers().getUserList().forEach(user -> userController
                        .syncUser(ProtoConverter.createJpaUser(user)));
                applicationController.updateModelVersion(event.getUsers().getVersion());
            }
        }
    }

    @EventListener
    public void on(ClusterEvents.AxonHubInstanceConnected event) {
        if (applicationController.getModelVersion() < event.getModelVersion()) {
            event.getRemoteConnection().requestUsers();
        }
    }

}
