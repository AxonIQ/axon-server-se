package io.axoniq.axonserver.enterprise.logconsumer;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.access.application.AppEvents;
import io.axoniq.axonserver.access.application.ApplicationController;
import io.axoniq.axonserver.grpc.ApplicationProtoConverter;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.Application;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import static io.axoniq.axonserver.RaftAdminGroup.isAdmin;

/**
 * @author Marc Gathier
 * <p>
 * Applies LogEntries with Application
 * Only run in Admin context
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
    public void consumeLogEntry(String groupId, Entry e) throws InvalidProtocolBufferException {
        // TODO: 6/12/2019 should we check for the group here? shouldn't apply entry be propagated to correct group members already?
//        if (!isAdmin(groupId)) {
//            return;
//        }
        if (entryType(e, Application.class.getName())) {
            Application application = Application.parseFrom(e.getSerializedObject().getData());
            applicationController.synchronize(ApplicationProtoConverter.createJpaApplication(application));
            applicationEventPublisher.publishEvent(new AppEvents.AppUpdated(application.getName()));
        }
    }
}
