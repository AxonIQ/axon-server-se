package io.axoniq.axonserver.enterprise.logconsumer;

import io.axoniq.axonserver.access.application.JpaContextApplicationController;
import io.axoniq.axonserver.cluster.LogEntryConsumer;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.ContextApplication;
import org.springframework.stereotype.Component;

/**
 * Consumer for log entries containing DELETE_CONTEXT_APPLICATION objects.
 *
 * @author Marc Gathier
 * @since 4.2
 */
@Component
public class DeleteContextApplicationConsumer implements LogEntryConsumer {

    public static final String ENTRY_TYPE = "DELETE_CONTEXT_APPLICATION";
    private final JpaContextApplicationController applicationController;

    public DeleteContextApplicationConsumer(
            JpaContextApplicationController applicationController) {
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
