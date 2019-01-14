package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.UserSynchronizationEvents;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.grpc.ProtoConverter;
import io.axoniq.platform.application.ApplicationModelController;
import io.axoniq.platform.user.User;
import io.axoniq.platform.user.UserController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Controller;

/**
 * @author Marc Gathier
 */
@Controller
public class UserSynchronizer {
    private final Logger logger = LoggerFactory.getLogger(UserSynchronizer.class);
    private final UserController userController;
    private final ApplicationModelController applicationController;

    public UserSynchronizer(UserController userController, ApplicationModelController applicationController) {
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
            logger.debug("UsersReceived, user version {}, mine {}", event.getUsers().getVersion(),
                        applicationController.getModelVersion(User.class) );
            if( applicationController.getModelVersion(User.class) < event.getUsers().getVersion()) {
                userController.clearUsers();
                event.getUsers().getUserList().forEach(user -> userController
                        .syncUser(ProtoConverter.createJpaUser(user)));
                applicationController.updateModelVersion(User.class, event.getUsers().getVersion());
            }
        }
    }

    @EventListener
    public void on(ClusterEvents.AxonServerInstanceConnected event) {
        if (applicationController.getModelVersion(User.class) < event.getModelVersion(User.class.getName())) {
            event.getRemoteConnection().requestUsers();
        }
    }

}
