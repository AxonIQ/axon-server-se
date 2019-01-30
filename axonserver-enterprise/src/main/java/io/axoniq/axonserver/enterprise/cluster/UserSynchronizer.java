package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.access.jpa.User;
import io.axoniq.axonserver.access.modelversion.ModelVersionController;
import io.axoniq.axonserver.access.user.UserController;
import io.axoniq.axonserver.applicationevents.UserEvents;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.grpc.UserProtoConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Controller;

/**
 * @author Marc Gathier
 */
@Controller
public class UserSynchronizer {
    private final Logger logger = LoggerFactory.getLogger(UserSynchronizer.class);
    private final UserController userController;
    private final ModelVersionController applicationController;
    private final ApplicationEventPublisher eventPublisher;

    public UserSynchronizer(UserController userController, ModelVersionController applicationController, ApplicationEventPublisher eventPublisher) {
        this.userController = userController;
        this.applicationController = applicationController;
        this.eventPublisher = eventPublisher;
    }


    @EventListener
    public void on(UserSynchronizationEvents.UserReceived event) {
        try {
            switch (event.getUser().getAction()) {
                case MERGE:
                    mergeUser(event.getUser());
                    break;
                case DELETE:
                    userController.deleteUser(event.getUser().getName());
                    eventPublisher.publishEvent(new UserEvents.UserDeleted(event.getUser().getName(), true));
                    break;
                default:
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
                event.getUsers().getUserList().forEach(this::mergeUser);
                applicationController.updateModelVersion(User.class, event.getUsers().getVersion());
            }
        }
    }

    private void mergeUser(io.axoniq.axonserver.grpc.internal.User user) {
        User mergeUser = UserProtoConverter.createJpaUser(user);
        userController.syncUser(mergeUser);
        eventPublisher.publishEvent(new UserEvents.UserUpdated(mergeUser, true));
    }

    @EventListener
    public void on(ClusterEvents.AxonServerInstanceConnected event) {
        if (applicationController.getModelVersion(User.class) < event.getModelVersion(User.class.getName())) {
            event.getRemoteConnection().requestUsers();
        }
    }

}
