package io.axoniq.axonserver.enterprise.replication.logconsumer;

import io.axoniq.axonserver.access.user.ReplicationGroupUserController;
import io.axoniq.axonserver.cluster.LogEntryConsumer;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.ContextUser;
import org.springframework.stereotype.Component;

/**
 * Consumer for log entries containing DELETE_CONTEXT_USER objects at replication group level.
 *
 * @author Marc Gathier
 * @since 4.1
 */
@Component
public class DeleteUserConsumer implements LogEntryConsumer {

    public static final String ENTRY_TYPE = "DELETE_CONTEXT_USER";

    private final ReplicationGroupUserController jpaContextUserController;

    public DeleteUserConsumer(
            ReplicationGroupUserController jpaContextUserController) {
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
