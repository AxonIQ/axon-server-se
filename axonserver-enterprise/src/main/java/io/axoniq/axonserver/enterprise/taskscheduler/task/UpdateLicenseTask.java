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
import io.axoniq.axonserver.grpc.internal.DistributeLicense;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

/**
 * Task that sends command to nodes that will create/update license
 *
 * @author Stefan Dragisic
 * @since 4.4
 */
@Component
public class UpdateLicenseTask implements ScheduledTask {

    private final TaskPayloadSerializer taskPayloadSerializer;

    private final ClusterController clusterController;

    private final ApplicationEventPublisher eventPublisher;

    public UpdateLicenseTask(TaskPayloadSerializer taskPayloadSerializer, ClusterController clusterController, ApplicationEventPublisher eventPublisher) {
        this.taskPayloadSerializer = taskPayloadSerializer;
        this.clusterController = clusterController;
        this.eventPublisher = eventPublisher;
    }

    @Override
    public void execute(Object payload) {

        UpdateLicenseTaskPayload licenseTaskPayload = (UpdateLicenseTaskPayload) taskPayloadSerializer.deserialize((Payload) payload);

        String payloadNodeName = licenseTaskPayload.getNodeName();
        String thisNodeName = clusterController.getMe().getName();

        if(thisNodeName.equals(payloadNodeName)) {
            eventPublisher.publishEvent(new ClusterEvents.LicenseUpdated(
                    licenseTaskPayload.getLicensePayload()) {
            });
        } else {
            clusterController
                    .getRemoteConnections()
                    .stream()
                    .filter(RemoteConnection::isConnected)
                    .filter(it->it.getClusterNode().getName().equals(payloadNodeName))
                    .findFirst()
                    .<Runnable>map(it -> (() -> it.publish(createCommand(licenseTaskPayload))))
                    .orElseThrow(() -> new TransientException("Node '"+ payloadNodeName+ "' not active. Scheduling update license task for later..."+" Task sent from: "+ thisNodeName))
                    .run();
        }

    }

    private ConnectorCommand createCommand(UpdateLicenseTaskPayload licenseTaskPayload) {
        return ConnectorCommand.newBuilder()
                .setDistributeLicense(
                        DistributeLicense.newBuilder()
                                .setLicense(ByteString.copyFrom(licenseTaskPayload.getLicensePayload()))
                                .build())
                .build();
    }
}
