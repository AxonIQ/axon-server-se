package io.axoniq.axonserver.enterprise.task;

import io.axoniq.axonserver.cluster.LogEntryConsumer;
import io.axoniq.axonserver.enterprise.jpa.Task;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.tasks.ScheduleTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Consumer for {@link ScheduleTask} log entries. Creates an entry in the controldb.
 * @author Marc Gathier
 * @since 4.3
 */
@Component
public class ScheduleTaskConsumer implements LogEntryConsumer {

    private final Logger logger = LoggerFactory.getLogger(ScheduleTaskConsumer.class);

    private final TaskRepository taskRepository;

    public ScheduleTaskConsumer(TaskRepository taskRepository) {
        this.taskRepository = taskRepository;
    }

    @Override
    public String entryType() {
        return ScheduleTask.class.getName();
    }

    @Override
    @Transactional
    public void consumeLogEntry(String groupId, Entry entry) throws Exception {
        ScheduleTask scheduleTask = ScheduleTask.parseFrom(entry.getSerializedObject().getData());
        logger.debug("Received task: {} for {}", scheduleTask.getTaskId(), scheduleTask.getTaskExecutor());
        Task task = new Task(scheduleTask);
        taskRepository.save(task);
    }
}
