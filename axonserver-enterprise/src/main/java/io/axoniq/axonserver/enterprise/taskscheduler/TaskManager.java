package io.axoniq.axonserver.enterprise.taskscheduler;

import io.axoniq.axonserver.enterprise.ContextEvents;
import io.axoniq.axonserver.enterprise.cluster.RaftLeaderProvider;
import io.axoniq.axonserver.enterprise.jpa.Task;
import io.axoniq.axonserver.grpc.tasks.ScheduleTask;
import io.axoniq.axonserver.grpc.tasks.Status;
import io.axoniq.axonserver.grpc.tasks.UpdateTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
public class TaskManager implements SmartLifecycle {

    private static final long MAX_RETRY_INTERVAL = TimeUnit.MINUTES.toMillis(1);
    private final Logger logger = LoggerFactory.getLogger(TaskManager.class);

    private final ScheduledTaskExecutor taskExecutor;
    private final TaskRepository taskRepository;
    private final RaftLeaderProvider raftLeaderProvider;
    private final TaskResultPublisher taskResultPublisher;
    private final Clock clock;
    private final Map<String, String> pendingTasks = new HashMap<>();
    private boolean running;

    public TaskManager(ScheduledTaskExecutor taskExecutor,
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
    @Scheduled(fixedDelayString = "${axoniq.axonserver.task-manager-delay:1000}", initialDelayString = "${axoniq.axonserver.task-manager-initial-delay:10000}")
    @Transactional
    public void checkForTasks() {
        if (!running) {
            return;
        }
        try {
            Set<String> leaderFor = raftLeaderProvider.leaderFor();
            taskRepository.findExecutableTasks(clock.millis(), leaderFor)
                          .forEach(this::executeTask);
        } catch (Exception ex) {
            logger.warn("Failed to run executable tasks", ex);
        }
    }

    /**
     * Stores a new task in the repository to be executed by the scheduler
     *
     * @param context      the context for the task
     * @param scheduleTask the task to schedule
     */
    @Transactional
    public void schedule(String context, ScheduleTask scheduleTask) {
        Task task = new Task(context, scheduleTask);
        taskRepository.save(task);
    }

    /**
     * Updates a task in the repository. If new status is COMPLETED, it will delete the task from the database
     *
     * @param updateTask updated task information
     */
    @Transactional
    public void updateTask(UpdateTask updateTask) {
        taskRepository.findFirstByTaskId(updateTask.getTaskId()).ifPresent(t -> update(t, updateTask));
    }

    @Transactional
    public void deleteAllByContext(String context) {
        taskRepository.deleteAllByContext(context);
    }

    public List<Task> findAllByContext(String context) {
        return taskRepository.findAllByContext(context);
    }


    @EventListener
    @Transactional
    public void on(ContextEvents.ContextDeleted contextDeleted) {
        logger.debug("Deleting tasks for {}", contextDeleted.getContext());
        deleteAllByContext(contextDeleted.getContext());
    }

    @EventListener
    @Transactional
    public void on(ContextEvents.AdminContextDeleted contextDeleted) {
        logger.debug("Deleting tasks for _admin");
        deleteAllByContext(getAdmin());
    }

    private void executeTask(Task task) {
        if (pendingTasks.containsKey(task.getTaskId()) || !raftLeaderProvider.isLeader(task.getContext())) {
            return;
        }

        if (logger.isDebugEnabled()) {
            logger.debug("{}: Execute task {}: {} planned execution {}ms ago",
                         task.getContext(),
                         task.getTaskId(),
                         task.getTaskExecutor(),
                         clock.millis() - task.getTimestamp()
            );
        }

        pendingTasks.put(task.getTaskId(), task.getTaskId());
        try {
            taskExecutor.executeTask(task)
                        .thenAccept(r -> completed(task))
                        .exceptionally(t -> {
                            error(task, t);
                            return null;
                        });
        } catch (Exception ex) {
            error(task, ex);
        }
    }

    private void error(Task task, Throwable cause) {
        logger.warn("{}: Failed to execute task {}: {}", task.getContext(),
                    task.getTaskId(),
                    task.getTaskExecutor(),
                    cause);
        CompletableFuture<Void> publishResultFuture;
        if (isTransient(cause)) {
            publishResultFuture = taskResultPublisher.publishResult(task.getContext(),
                                                                    task.getTaskId(),
                                                                    Status.SCHEDULED,
                                                                    newSchedule(task),
                                                                    Math.min(task.getRetryInterval() * 2,
                                                                             MAX_RETRY_INTERVAL));
        } else {
            publishResultFuture = taskResultPublisher.publishResult(task.getContext(),
                                                                    task.getTaskId(),
                                                                    Status.FAILED,
                                                                    clock.millis(),
                                                                    0);
        }

        publishResultFuture.thenAccept(r -> pendingTasks.remove(task.getTaskId()))
                           .exceptionally(e -> {
                               logger.warn("{}: Failed to publish result for task {}: {}", task.getContext(),
                                           task.getTaskId(),
                                           task.getTaskExecutor(),
                                           e);
                               return null;
                           });
    }

    private boolean isTransient(Throwable cause) {
        if (cause == null) {
            return false;
        }
        if (cause instanceof TransientException) {
            return true;
        }
        return isTransient(cause.getCause());
    }

    private long newSchedule(Task task) {
        return clock.millis() + Math.min(task.getRetryInterval(), MAX_RETRY_INTERVAL);
    }

    private void completed(Task task) {
        taskResultPublisher.publishResult(task.getContext(),
                                          task.getTaskId(),
                                          Status.COMPLETED,
                                          clock.millis(),
                                          0)
                           .thenAccept(r -> pendingTasks.remove(task.getTaskId()))
                           .exceptionally(e -> {
                               logger.warn("{}: Failed to publish result for completed task {}: {}", task.getContext(),
                                           task.getTaskId(),
                                           task.getTaskExecutor(),
                                           e);
                               return null;
                           });
    }

    private void update(Task task, UpdateTask updateTask) {
        if (Status.COMPLETED.equals(updateTask.getStatus())) {
            taskRepository.delete(task);
            return;
        }

        task.setTimestamp(updateTask.getInstant());
        task.setRetryInterval(updateTask.getRetryInterval());
        task.setStatus(updateTask.getStatus());
        taskRepository.save(task);
    }


    @Override
    public void start() {
        running = true;
    }

    @Override
    public void stop() {
        logger.info("Stop TaskManager");
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
