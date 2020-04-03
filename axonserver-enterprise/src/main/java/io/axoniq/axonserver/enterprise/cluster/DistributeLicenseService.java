package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.enterprise.taskscheduler.TaskPublisher;
import io.axoniq.axonserver.enterprise.taskscheduler.task.PrepareUpdateLicenseTask;
import org.springframework.stereotype.Component;

import java.time.Duration;

import static io.axoniq.axonserver.RaftAdminGroup.getAdmin;

/**
 * Triggers distribution of license across cluster
 *
 * @author Stefan Dragisic
 */
@Component
public class DistributeLicenseService {

    private final TaskPublisher taskPublisher;

    public DistributeLicenseService(TaskPublisher taskPublisher) {
        this.taskPublisher = taskPublisher;
    }

    public void distributeLicense(byte[] license) {
        taskPublisher.publishScheduledTask(getAdmin(),
                PrepareUpdateLicenseTask.class
                        .getName(),
                license,
                Duration.ZERO);
    }


}
