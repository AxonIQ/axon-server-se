package io.axoniq.axonserver.enterprise.logconsumer;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.access.user.UserController;
import io.axoniq.axonserver.applicationevents.UserEvents;
import io.axoniq.axonserver.cluster.LogEntryConsumer;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

/**
 * Deletes user.
 *
 * @author Marc Gathier
 */
@Component
public class DeleteUserConsumer implements LogEntryConsumer {

    public static final String DELETE_USER = "DELETE_USER";
    private final Logger logger = LoggerFactory.getLogger(DeleteUserConsumer.class);
    private final UserController userController;
    private final ApplicationEventPublisher applicationEventPublisher;

    public DeleteUserConsumer(UserController userController,
                              ApplicationEventPublisher applicationEventPublisher) {
        this.userController = userController;
        this.applicationEventPublisher = applicationEventPublisher;
    }

    @Override
    public String entryType() {
        return DELETE_USER;
    }

    @Override
    public void consumeLogEntry(String groupId, Entry e) throws InvalidProtocolBufferException {
        User user = User.parseFrom(e.getSerializedObject().getData());
        logger.debug("{}: Delete user: {}", groupId, user.getName());
        userController.deleteUser(user.getName());
        applicationEventPublisher.publishEvent(new UserEvents.UserDeleted(user.getName(), false));
    }
}
