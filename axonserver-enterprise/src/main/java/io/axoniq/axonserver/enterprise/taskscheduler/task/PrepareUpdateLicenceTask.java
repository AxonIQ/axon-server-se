package io.axoniq.axonserver.enterprise.taskscheduler.task;

import io.axoniq.axonserver.enterprise.taskscheduler.ScheduledTask;
import io.axoniq.axonserver.enterprise.taskscheduler.TaskPublisher;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletableFuture;

import static io.axoniq.axonserver.RaftAdminGroup.getAdmin;

/**
 * TODO
 *
 * @author Stefan Dragisic
 */
@Component
public class PrepareUpdateLicenceTask implements ScheduledTask {

    private final TaskPublisher taskPublisher;

    public PrepareUpdateLicenceTask(
            TaskPublisher taskPublisher) {
        this.taskPublisher = taskPublisher;
    }

    /**
     * TODO
     */
    @Override
    public CompletableFuture<Void> executeAsync(Object payload) {
        return taskPublisher.publishScheduledTask(getAdmin(), DelegateLicenceUpdateTask.class.getName(),
                payload,
                Duration.of(100, ChronoUnit.MILLIS));
    }
}
