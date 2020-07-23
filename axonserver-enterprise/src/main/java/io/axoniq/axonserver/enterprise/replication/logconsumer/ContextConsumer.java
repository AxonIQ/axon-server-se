package io.axoniq.axonserver.enterprise.replication.logconsumer;

import io.axoniq.axonserver.cluster.LogEntryConsumer;
import io.axoniq.axonserver.enterprise.replication.group.ReplicationGroupController;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupContext;
import org.springframework.stereotype.Component;

/**
 * Consumer of log entries containing {@link ReplicationGroupContext} objects  at replication group level.
 *
 * @author Marc Gathier
 * @since 4.4
 */
@Component
public class ContextConsumer implements LogEntryConsumer {

    private final ReplicationGroupController replicationGroupController;

    public ContextConsumer(
            ReplicationGroupController replicationGroupController) {
        this.replicationGroupController = replicationGroupController;
    }

    @Override
    public String entryType() {
        return ReplicationGroupContext.class.getName();
    }

    @Override
    public void consumeLogEntry(String groupId, Entry entry) throws Exception {
        ReplicationGroupContext context = ReplicationGroupContext.parseFrom(entry.getSerializedObject().getData());
        replicationGroupController.addContext(groupId, context.getContextName(), context.getMetaDataMap());
    }
}
