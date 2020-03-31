package io.axoniq.axonserver.enterprise.taskscheduler.task;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.cluster.internal.RemoteConnection;
import io.axoniq.axonserver.enterprise.jpa.Payload;
import io.axoniq.axonserver.enterprise.taskscheduler.ScheduledTask;
import io.axoniq.axonserver.enterprise.taskscheduler.TaskPayloadSerializer;
import io.axoniq.axonserver.enterprise.taskscheduler.TransientException;
import io.axoniq.axonserver.grpc.internal.ConnectorCommand;
import io.axoniq.axonserver.grpc.internal.DistributeLicence;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

/**
 * TODO
 *
 * @author Stefan Dragisic
 */
@Component
public class UpdateLicenceTask implements ScheduledTask {

    private final TaskPayloadSerializer taskPayloadSerializer;

    private final ClusterController clusterController;

    private final ApplicationEventPublisher eventPublisher;

    public UpdateLicenceTask(TaskPayloadSerializer taskPayloadSerializer, ClusterController clusterController, ApplicationEventPublisher eventPublisher) {
        this.taskPayloadSerializer = taskPayloadSerializer;
        this.clusterController = clusterController;
        this.eventPublisher = eventPublisher;
    }

    @Override
    public void execute(Object payload) {

        UpdateLicenceTaskPayload licenceTaskPayload = (UpdateLicenceTaskPayload) taskPayloadSerializer.deserialize((Payload) payload);

        String payloadNodeName = licenceTaskPayload.getNodeName();
        String thisNodeName = clusterController.getMe().getName();

        if(thisNodeName.equals(payloadNodeName)) {
            eventPublisher.publishEvent(new ClusterEvents.LicenceUpdated(
                    licenceTaskPayload.getLicencePayload()) {
            });
        } else {
            clusterController
                    .getRemoteConnections()
                    .stream()
                    .filter(RemoteConnection::isConnected)
                    .filter(it->it.getClusterNode().getName().equals(payloadNodeName))
                    .findFirst()
                    .<Runnable>map(it -> (() -> it.publish(createCommand(licenceTaskPayload))))
                    .orElseThrow(() -> new TransientException("Node '"+ payloadNodeName+ "' not active. Scheduling update licence task for later..."+" Task sent from: "+ thisNodeName))
                    .run();
        }

    }

    private ConnectorCommand createCommand(UpdateLicenceTaskPayload licenceTaskPayload) {
        return ConnectorCommand.newBuilder()
                .setDistributeLicence(
                        DistributeLicence.newBuilder()
                                .setLicence(ByteString.copyFrom(licenceTaskPayload.getLicencePayload()))
                                .build())
                .build();
    }
}
