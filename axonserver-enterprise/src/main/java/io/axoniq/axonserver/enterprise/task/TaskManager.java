package io.axoniq.axonserver.enterprise.task;

import io.axoniq.axonserver.enterprise.cluster.RaftLeaderProvider;
import io.axoniq.axonserver.enterprise.jpa.Task;
import io.axoniq.axonserver.grpc.tasks.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import static io.axoniq.axonserver.RaftAdminGroup.getAdmin;

/**
 * @author Marc Gathier
 */
@Component
public class TaskManager {

    private final Logger logger = LoggerFactory.getLogger(TaskManager.class);

    private final AdminTaskExecutor taskExecutor;
    private final TaskRepository taskRepository;
    private final RaftLeaderProvider raftLeaderProvider;
    private final TaskResultPublisher taskResultPublisher;


    public TaskManager(AdminTaskExecutor taskExecutor, TaskRepository taskRepository,
                       RaftLeaderProvider raftLeaderProvider,
                       TaskResultPublisher taskResultPublisher) {
        this.taskExecutor = taskExecutor;
        this.taskRepository = taskRepository;
        this.raftLeaderProvider = raftLeaderProvider;
        this.taskResultPublisher = taskResultPublisher;
    }

    @Scheduled(fixedDelayString = "1000", initialDelayString = "10000")
    @Transactional
    public void checkForTasks() {
        if (!raftLeaderProvider.isLeader(getAdmin())) {
            return;
        }

        try {
            taskRepository.findExecutableTasks(System.currentTimeMillis())
                          .forEach(this::executeTask);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void executeTask(Task task) {
        try {
            taskExecutor.executeTask(task);
            task.setStatus(Status.COMPLETED);
        } catch (TransientException transientException) {
            logger.error("Failed to execute task", transientException);
            task.setTimestamp(System.currentTimeMillis() + 1000);
        } catch (Exception e) {
            logger.error("Failed to execute task", e);
            task.setStatus(Status.FAILED);
        }
        taskRepository.save(task);
        taskResultPublisher.publishResult(task, task.getStatus(), 0L);
    }
}
