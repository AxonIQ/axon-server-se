package io.axoniq.axonserver.enterprise.replication.logconsumer;

import io.axoniq.axonserver.cluster.LogEntryConsumer;
import io.axoniq.axonserver.enterprise.replication.admin.AdminReplicationGroupController;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupContext;
import org.springframework.stereotype.Component;

/**
 * Consumer of log entries containing {@link ReplicationGroupContext} objects. Applied in the _admin replication
 * group to update the tables used in Axon Server console.
 *
 * @author Marc Gathier
 * @since 4.2
 */
@Component
public class AdminContextConsumer implements LogEntryConsumer {

    public static final String ENTRY_TYPE = "ADD_CONTEXT";

    private final AdminReplicationGroupController adminReplicationGroupController;

    public AdminContextConsumer(
            AdminReplicationGroupController adminReplicationGroupController) {
        this.adminReplicationGroupController = adminReplicationGroupController;
    }

    @Override
    public String entryType() {
        return ENTRY_TYPE;
    }

    @Override
    public void consumeLogEntry(String groupId, Entry entry) throws Exception {
        ReplicationGroupContext context = ReplicationGroupContext.parseFrom(entry.getSerializedObject().getData());
        adminReplicationGroupController.registerContext(context.getReplicationGroupName(),
                                                        context.getContextName(),
                                                        context.getMetaDataMap());
    }
}
