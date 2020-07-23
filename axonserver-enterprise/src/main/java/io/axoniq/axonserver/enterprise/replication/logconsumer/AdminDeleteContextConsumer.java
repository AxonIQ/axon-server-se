package io.axoniq.axonserver.enterprise.replication.logconsumer;

import io.axoniq.axonserver.cluster.LogEntryConsumer;
import io.axoniq.axonserver.enterprise.replication.admin.AdminReplicationGroupController;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.DeleteContextRequest;
import org.springframework.stereotype.Component;

/**
 * Applies a ADMIN_DELETE_CONTEXT log entry in the _admin replication group. Deletes the context from the
 * tables used by the Axon Server console.
 *
 * @author Marc Gathier
 * @since 4.1
 */
@Component
public class AdminDeleteContextConsumer implements LogEntryConsumer {

    public static final String ENTRY_TYPE = "ADMIN_DELETE_CONTEXT";
    private final AdminReplicationGroupController adminReplicationGroupController;

    public AdminDeleteContextConsumer(
            AdminReplicationGroupController adminReplicationGroupController) {
        this.adminReplicationGroupController = adminReplicationGroupController;
    }

    @Override
    public String entryType() {
        return ENTRY_TYPE;
    }

    @Override
    public void consumeLogEntry(String groupId, Entry entry) throws Exception {
        DeleteContextRequest context = DeleteContextRequest.parseFrom(entry.getSerializedObject().getData());
        adminReplicationGroupController.unregisterContext(context.getReplicationGroupName(),
                                                          context.getContext());
    }
}
