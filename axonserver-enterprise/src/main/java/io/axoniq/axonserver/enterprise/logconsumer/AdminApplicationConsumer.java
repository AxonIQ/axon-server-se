package io.axoniq.axonserver.enterprise.logconsumer;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.access.application.AppEvents;
import io.axoniq.axonserver.access.application.ApplicationController;
import io.axoniq.axonserver.cluster.LogEntryConsumer;
import io.axoniq.axonserver.grpc.ApplicationProtoConverter;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.Application;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

/**
 * Applies LogEntries with Application information. Runs in Admin context only.
 *
 * @author Marc Gathier
 */
@Component
public class AdminApplicationConsumer implements LogEntryConsumer {

    private final ApplicationController applicationController;
    private final ApplicationEventPublisher applicationEventPublisher;

    public AdminApplicationConsumer(ApplicationController applicationController,
                                    ApplicationEventPublisher applicationEventPublisher) {
        this.applicationController = applicationController;
        this.applicationEventPublisher = applicationEventPublisher;
    }

    @Override
    public String entryType() {
        return Application.class.getName();
    }

    @Override
    public void consumeLogEntry(String groupId, Entry e) throws InvalidProtocolBufferException {
        Application application = Application.parseFrom(e.getSerializedObject().getData());
        applicationController.synchronize(ApplicationProtoConverter.createJpaApplication(application));
        applicationEventPublisher.publishEvent(new AppEvents.AppUpdated(application.getName()));
    }
}
