package io.axoniq.axonserver.enterprise.taskscheduler.task;

import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.taskscheduler.ScheduledTask;
import io.axoniq.axonserver.enterprise.taskscheduler.TaskPayloadSerializer;
import io.axoniq.axonserver.enterprise.taskscheduler.TaskPublisher;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletableFuture;

import static io.axoniq.axonserver.RaftAdminGroup.getAdmin;

/**
 * Executes on admin node and schedules a task to create/update license on each node.
 *
 * @author Stefan Dragisic
 * @since 4.4
 */
@Component
public class PrepareUpdateLicenseTask implements ScheduledTask {

    private final TaskPublisher taskPublisher;

    private final TaskPayloadSerializer taskPayloadSerializer;
    private final ClusterController clusterController;

    public PrepareUpdateLicenseTask(TaskPublisher taskPublisher, TaskPayloadSerializer taskPayloadSerializer, ClusterController clusterController) {
        this.taskPublisher = taskPublisher;

        this.taskPayloadSerializer = taskPayloadSerializer;
        this.clusterController = clusterController;
    }

    @Override
    public void execute(Object payload) {
        byte[] licensePayload = (byte[]) payload;

        clusterController.nodes()
                .map(node -> new UpdateLicenseTaskPayload(node.getName(),licensePayload))
                .map(taskPayloadSerializer::serialize)
                .forEach(task->taskPublisher.publishScheduledTask(getAdmin(), UpdateLicenseTask.class.getName(),task, Duration.ZERO));
    }

}
