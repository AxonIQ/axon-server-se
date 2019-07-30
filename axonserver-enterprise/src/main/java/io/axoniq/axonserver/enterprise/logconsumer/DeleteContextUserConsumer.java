package io.axoniq.axonserver.enterprise.logconsumer;

import io.axoniq.axonserver.access.application.JpaContextUserController;
import io.axoniq.axonserver.cluster.LogEntryConsumer;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.ContextUser;
import org.springframework.stereotype.Component;

/**
 * Consumer for log entries containing DELETE_CONTEXT_USER objects.
 *
 * @author Marc Gathier
 */
@Component
public class DeleteContextUserConsumer implements LogEntryConsumer {

    public static final String ENTRY_TYPE = "DELETE_CONTEXT_USER";

    private final JpaContextUserController jpaContextUserController;

    public DeleteContextUserConsumer(
            JpaContextUserController jpaContextUserController) {
        this.jpaContextUserController = jpaContextUserController;
    }

    @Override
    public String entryType() {
        return ENTRY_TYPE;
    }

    @Override
    public void consumeLogEntry(String groupId, Entry entry) throws Exception {
        ContextUser user = ContextUser.parseFrom(entry.getSerializedObject().getData());
        jpaContextUserController.deleteUser(user.getContext(), user.getUser().getName());
    }
}
