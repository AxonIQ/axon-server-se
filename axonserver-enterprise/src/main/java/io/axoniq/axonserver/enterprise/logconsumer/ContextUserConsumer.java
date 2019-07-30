package io.axoniq.axonserver.enterprise.logconsumer;

import io.axoniq.axonserver.access.application.JpaContextUserController;
import io.axoniq.axonserver.cluster.LogEntryConsumer;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.ContextUser;
import org.springframework.stereotype.Component;

/**
 * Consumer of log entries containing {@link ContextUser} objects.
 *
 * @author Marc Gathier
 * @since 4.2
 */
@Component
public class ContextUserConsumer implements LogEntryConsumer {

    private final JpaContextUserController jpaContextUserController;

    public ContextUserConsumer(
            JpaContextUserController jpaContextUserController) {
        this.jpaContextUserController = jpaContextUserController;
    }

    @Override
    public String entryType() {
        return ContextUser.class.getName();
    }

    @Override
    public void consumeLogEntry(String groupId, Entry entry) throws Exception {
        ContextUser user = ContextUser.parseFrom(entry.getSerializedObject().getData());
        jpaContextUserController.mergeUser(user.getContext(), user.getUser());
    }
}
