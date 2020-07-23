package io.axoniq.axonserver.enterprise.replication.logconsumer;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.access.application.ReplicationGroupApplicationController;
import io.axoniq.axonserver.cluster.LogEntryConsumer;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.ContextApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Applies entries in regard to application table at replication group level.
 *
 * @author Marc Gathier
 * @since 4.1
 */
@Component
public class ApplicationConsumer implements LogEntryConsumer {

    private final Logger logger = LoggerFactory.getLogger(ApplicationConsumer.class);
    private final ReplicationGroupApplicationController applicationController;

    public ApplicationConsumer(ReplicationGroupApplicationController applicationController) {
        this.applicationController = applicationController;
    }

    @Override
    public String entryType() {
        return ContextApplication.class.getName();
    }

    @Override
    public void consumeLogEntry(String groupId, Entry entry) throws InvalidProtocolBufferException {
        ContextApplication application = ContextApplication.parseFrom(entry.getSerializedObject().getData());
        applicationController.mergeApplication(application);
    }
}
