package io.axoniq.axonserver.enterprise.taskscheduler;

import io.axoniq.axonserver.cluster.LogEntryConsumer;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.tasks.UpdateTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Consumes an update task request from the RAFT log. Updates the task with the new information.
 * If the new status is completed, it deletes the task from the repository.
 *
 * @author Marc Gathier
 * @since 4.3
 */
@Component
public class UpdateTaskConsumer implements LogEntryConsumer {

    private final Logger logger = LoggerFactory.getLogger(UpdateTaskConsumer.class);
    private final TaskManager taskManager;

    /**
     * Constructor for the log consumer.
     *
     * @param taskManager the task manager
     */
    public UpdateTaskConsumer(TaskManager taskManager) {
        this.taskManager = taskManager;
    }

    @Override
    public String entryType() {
        return UpdateTask.class.getName();
    }

    @Override
    public void consumeLogEntry(String groupId, Entry entry) throws Exception {
        try {
            UpdateTask updateTask = UpdateTask.parseFrom(entry.getSerializedObject().getData());
            logger.debug("Received updated task: {} status {} retry {}",
                         updateTask.getTaskId(),
                         updateTask.getStatus(),
                         updateTask.getRetryInterval());
            taskManager.updateTask(updateTask);
        } catch (RuntimeException ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
}
