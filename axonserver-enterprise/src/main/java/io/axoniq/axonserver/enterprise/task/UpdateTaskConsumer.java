package io.axoniq.axonserver.enterprise.task;

import io.axoniq.axonserver.cluster.LogEntryConsumer;
import io.axoniq.axonserver.enterprise.jpa.Task;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.tasks.Status;
import io.axoniq.axonserver.grpc.tasks.UpdateTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * @author Marc Gathier
 */
@Component
public class UpdateTaskConsumer implements LogEntryConsumer {

    private final Logger logger = LoggerFactory.getLogger(UpdateTaskConsumer.class);
    private final TaskRepository taskRepository;

    public UpdateTaskConsumer(TaskRepository taskRepository) {
        this.taskRepository = taskRepository;
    }

    @Override
    public String entryType() {
        return UpdateTask.class.getName();
    }

    @Override
    @Transactional
    public void consumeLogEntry(String groupId, Entry entry) throws Exception {
        UpdateTask updateTask = UpdateTask.parseFrom(entry.getSerializedObject().getData());
        logger.warn("Received updated task: {} status {}", updateTask.getTaskId(), updateTask.getStatus());
        taskRepository.findFirstByTaskId(updateTask.getTaskId()).ifPresent(t -> update(t, updateTask));
    }

    private void update(Task task, UpdateTask updateTask) {
        if (updateTask.getInstant() > 0) {
            task.setTimestamp(updateTask.getInstant());
            taskRepository.save(task);
        } else {
            if (Status.COMPLETED.equals(updateTask.getStatus())) {
                taskRepository.delete(task);
            }
        }
    }
}
