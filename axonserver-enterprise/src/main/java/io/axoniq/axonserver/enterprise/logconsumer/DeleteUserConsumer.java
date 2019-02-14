package io.axoniq.axonserver.enterprise.logconsumer;

import io.axoniq.axonserver.access.user.UserController;
import io.axoniq.axonserver.applicationevents.UserEvents;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

/**
 * Author: marc
 */
@Component
public class DeleteUserConsumer implements LogEntryConsumer {
    public static final String DELETE_USER  = "DELETE_USER";
    private final Logger logger = LoggerFactory.getLogger(DeleteUserConsumer.class);
    private final UserController userController;
    private final ApplicationEventPublisher applicationEventPublisher;

    public DeleteUserConsumer(UserController userController,
                              ApplicationEventPublisher applicationEventPublisher) {
        this.userController = userController;
        this.applicationEventPublisher = applicationEventPublisher;
    }

    @Override
    public void consumeLogEntry(String groupId, Entry e) {
        if( entryType(e, DELETE_USER)) {
            User user = null;
            try {
                user = User.parseFrom(e.getSerializedObject().getData());
                logger.debug("{}: Delete user: {}", groupId, user.getName());
                userController.deleteUser(user.getName());
                applicationEventPublisher.publishEvent(new UserEvents.UserDeleted(user.getName(), false));
            } catch (Exception e1) {
                logger.warn("Failed to update user: {}", user, e1);
            }
        }
    }
}
