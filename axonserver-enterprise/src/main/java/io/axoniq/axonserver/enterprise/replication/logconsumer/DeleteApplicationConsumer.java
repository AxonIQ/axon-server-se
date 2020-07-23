package io.axoniq.axonserver.enterprise.replication.logconsumer;

import io.axoniq.axonserver.access.application.ReplicationGroupApplicationController;
import io.axoniq.axonserver.cluster.LogEntryConsumer;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.ContextApplication;
import org.springframework.stereotype.Component;

/**
 * Consumer for log entries containing DELETE_CONTEXT_APPLICATION objects at replication group level
 *
 * @author Marc Gathier
 * @since 4.2
 */
@Component
public class DeleteApplicationConsumer implements LogEntryConsumer {

    public static final String ENTRY_TYPE = "DELETE_CONTEXT_APPLICATION";
    private final ReplicationGroupApplicationController applicationController;

    public DeleteApplicationConsumer(
            ReplicationGroupApplicationController applicationController) {
        this.applicationController = applicationController;
    }

    @Override
    public String entryType() {
        return ENTRY_TYPE;
    }

    @Override
    public void consumeLogEntry(String groupId, Entry entry) throws Exception {
        ContextApplication app = ContextApplication.parseFrom(entry.getSerializedObject().getData());
        applicationController.deleteApplication(app.getContext(), app.getName());
    }
}
