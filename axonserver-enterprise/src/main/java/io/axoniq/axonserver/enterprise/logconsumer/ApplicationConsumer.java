package io.axoniq.axonserver.enterprise.logconsumer;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.access.application.JpaContextApplicationController;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.ContextApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * @author Marc Gathier
 */
@Component
public class ApplicationConsumer implements LogEntryConsumer {

    private final Logger logger = LoggerFactory.getLogger(ApplicationConsumer.class);
    private final JpaContextApplicationController applicationController;

    public ApplicationConsumer(JpaContextApplicationController applicationController) {
        this.applicationController = applicationController;
    }

    @Override
    public void consumeLogEntry(String groupId, Entry entry) throws InvalidProtocolBufferException {
        if (entryType(entry, ContextApplication.class.getName())) {
            ContextApplication application = ContextApplication.parseFrom(entry.getSerializedObject().getData());
            applicationController.mergeApplication(application);
        }
    }
}
