package io.axoniq.axonserver.enterprise.task;

import io.axoniq.axonserver.enterprise.cluster.RaftLeaderProvider;
import io.axoniq.axonserver.enterprise.jpa.Task;
import io.axoniq.axonserver.grpc.tasks.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.axoniq.axonserver.RaftAdminGroup.getAdmin;

/**
 * Component that reads tasks from the controldb and tries to execute them.
 * It will only process tasks when the current node is the admin leader.
 *
 * @author Marc Gathier
 * @since 4.3
 */
@Component
public class TaskManager {

    private final Logger logger = LoggerFactory.getLogger(TaskManager.class);

    private final AdminTaskExecutor taskExecutor;
    private final TaskRepository taskRepository;
    private final RaftLeaderProvider raftLeaderProvider;
    private final TaskResultPublisher taskResultPublisher;
    private final Clock clock;


    public TaskManager(AdminTaskExecutor taskExecutor,
                       TaskRepository taskRepository,
                       RaftLeaderProvider raftLeaderProvider,
                       TaskResultPublisher taskResultPublisher,
                       Clock clock) {
        this.taskExecutor = taskExecutor;
        this.taskRepository = taskRepository;
        this.raftLeaderProvider = raftLeaderProvider;
        this.taskResultPublisher = taskResultPublisher;
        this.clock = clock;
    }

    /**
     * Checks if there are tasks available to run. If the current node is not the admin leader it does not perform any
     * actions.
     */
    @Scheduled(fixedDelayString = "1000", initialDelayString = "10000")
    @Transactional
    public void checkForTasks() {
        if (!raftLeaderProvider.isLeader(getAdmin())) {
            return;
        }

        try {
            taskRepository.findExecutableTasks(clock.millis())
                          .forEach(this::executeTask);
        } catch (Exception ex) {
            logger.warn("Failed to run executable tasks", ex);
        }
    }

    private void executeTask(Task task) {
        long retry = 0;
        Status status = task.getStatus();
        try {
            taskExecutor.executeTask(task);
            status = Status.COMPLETED;
        } catch (TransientException transientException) {
            logger.warn("Failed to execute task", transientException);
            retry = Optional.ofNullable(task.getErrorHandler().getRescheduleInterval()).orElse(1000L);
        } catch (Exception e) {
            logger.warn("Failed to execute task", e);
            status = Status.FAILED;
        }
        taskResultPublisher.publishResult(task.getTaskId(),
                                          status,
                                          clock.millis() + Math.min(retry, TimeUnit.MINUTES.toMillis(1)),
                                          retry * 2);
    }
}
