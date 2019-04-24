package io.axoniq.axonserver.enterprise.logconsumer;

import io.axoniq.axonserver.access.user.UserController;
import io.axoniq.axonserver.applicationevents.UserEvents;
import io.axoniq.axonserver.grpc.UserProtoConverter;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

/**
 * @author Marc Gathier
 */
@Component
public class UserConsumer implements LogEntryConsumer {
    private final Logger logger = LoggerFactory.getLogger(UserConsumer.class);
    private final UserController userController;
    private final ApplicationEventPublisher applicationEventPublisher;

    public UserConsumer(UserController userController, ApplicationEventPublisher applicationEventPublisher) {
        this.userController = userController;
        this.applicationEventPublisher = applicationEventPublisher;
    }

    @Override
    public void consumeLogEntry(String groupId, Entry e) {
        if( entryType(e, User.class.getName())) {
            User user = null;
            try {
                user = User.parseFrom(e.getSerializedObject().getData());
                logger.debug("{}: Received user: {}", groupId, user);
                io.axoniq.axonserver.access.jpa.User jpaUser = UserProtoConverter.createJpaUser(user);
                userController.syncUser(jpaUser);
                applicationEventPublisher.publishEvent(new UserEvents.UserUpdated(jpaUser, false));
            } catch (Exception e1) {
                logger.warn("Failed to update user: {}", user, e1);
            }
        }
    }
}
