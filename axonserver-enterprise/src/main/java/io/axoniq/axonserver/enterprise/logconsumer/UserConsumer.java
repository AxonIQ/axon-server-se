package io.axoniq.axonserver.enterprise.logconsumer;

import io.axoniq.axonserver.grpc.ProtoConverter;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.User;
import io.axoniq.platform.user.UserController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Author: marc
 */
@Component
public class UserConsumer implements LogEntryConsumer {
    private final Logger logger = LoggerFactory.getLogger(UserConsumer.class);
    private final UserController userController;

    public UserConsumer(UserController userController) {
        this.userController = userController;
    }

    @Override
    public void consumeLogEntry(String groupId, Entry e) {
        if( entryType(e, User.class.getName())) {
            User user = null;
            try {
                user = User.parseFrom(e.getSerializedObject().getData());
                logger.warn("Received user: {}", user);
                userController.syncUser(ProtoConverter.createJpaUser(user));
            } catch (Exception e1) {
                logger.warn("Failed to update user: {}", user, e1);
            }
        }
    }


    @Override
    public int priority() {
        return 0;
    }
}
