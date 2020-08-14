package io.axoniq.axonserver.enterprise.replication.logconsumer;

import io.axoniq.axonserver.cluster.LogEntryConsumer;
import io.axoniq.axonserver.enterprise.replication.group.ReplicationGroupController;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.DeleteContextRequest;
import org.springframework.stereotype.Component;

/**
 * Deletes a context at replication group level.
 *
 * @author Marc Gathier
 * @since 4.4
 */
@Component
public class DeleteContextConsumer implements LogEntryConsumer {

    private final ReplicationGroupController replicationGroupController;

    public DeleteContextConsumer(
            ReplicationGroupController replicationGroupController) {
        this.replicationGroupController = replicationGroupController;
    }

    @Override
    public String entryType() {
        return DeleteContextRequest.class.getName();
    }

    @Override
    public void consumeLogEntry(String groupId, Entry entry) throws Exception {
        DeleteContextRequest request = DeleteContextRequest.parseFrom(entry.getSerializedObject().getData());
        replicationGroupController.deleteContext(request.getContext(), groupId, request.getPreserveEventstore());
    }
}
