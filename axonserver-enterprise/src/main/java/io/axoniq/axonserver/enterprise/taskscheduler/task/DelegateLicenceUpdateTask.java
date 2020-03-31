package io.axoniq.axonserver.enterprise.taskscheduler.task;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.ClusterPublisher;
import io.axoniq.axonserver.enterprise.jpa.Payload;
import io.axoniq.axonserver.enterprise.taskscheduler.ScheduledTask;
import io.axoniq.axonserver.enterprise.taskscheduler.TaskPayloadSerializer;
import io.axoniq.axonserver.enterprise.taskscheduler.TaskPublisher;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.tasks.ScheduleTask;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.UUID;

import static io.axoniq.axonserver.RaftAdminGroup.getAdmin;

/**
 * TODO
 *
 * @author Stefan Dragisic
 */
@Component
public class DelegateLicenceUpdateTask implements ScheduledTask {

    private final TaskPublisher taskPublisher;

    private final TaskPayloadSerializer taskPayloadSerializer;
    private final ClusterController clusterController;

    public DelegateLicenceUpdateTask(TaskPublisher taskPublisher, TaskPayloadSerializer taskPayloadSerializer, ClusterController clusterController) {
        this.taskPublisher = taskPublisher;

        this.taskPayloadSerializer = taskPayloadSerializer;
        this.clusterController = clusterController;
    }

    @Override
    public void execute(Object payload) {
        byte[] licencePayload = (byte[]) payload;

        clusterController.nodes()
                .map(node -> new UpdateLicenceTaskPayload(node.getName(),licencePayload))
                .map(taskPayloadSerializer::serialize)
                .forEach(task->taskPublisher.publishScheduledTask(getAdmin(),UpdateLicenceTask.class.getName(),task, Duration.ZERO));
    }


}
