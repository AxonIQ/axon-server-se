package io.axoniq.axonserver.enterprise.taskscheduler;

import io.axoniq.axonserver.cluster.LogEntryConsumer;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.tasks.ScheduleTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Consumer for {@link ScheduleTask} log entries. Sends the task to the task manager to be scheduled.
 *
 * @author Marc Gathier
 * @since 4.3
 */
@Component
public class ScheduleTaskConsumer implements LogEntryConsumer {

    private final Logger logger = LoggerFactory.getLogger(ScheduleTaskConsumer.class);

    private final ClusterTaskManager taskManager;

    public ScheduleTaskConsumer(ClusterTaskManager taskManager) {
        this.taskManager = taskManager;
    }

    @Override
    public String entryType() {
        return ScheduleTask.class.getName();
    }

    @Override
    public void consumeLogEntry(String groupId, Entry entry) throws Exception {
        ScheduleTask scheduleTask = ScheduleTask.parseFrom(entry.getSerializedObject().getData());
        logger.debug("{}: Received task: {} for {}", groupId, scheduleTask.getTaskId(), scheduleTask.getTaskExecutor());
        taskManager.schedule(groupId, scheduleTask);
    }
}
