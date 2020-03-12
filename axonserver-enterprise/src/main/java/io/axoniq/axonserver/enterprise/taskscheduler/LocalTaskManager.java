package io.axoniq.axonserver.enterprise.taskscheduler;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.enterprise.jpa.Payload;
import io.axoniq.axonserver.enterprise.jpa.Task;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.tasks.ScheduleTask;
import io.axoniq.axonserver.grpc.tasks.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;

/**
 * Component that reads tasks from the controldb and tries to execute them.
 * It will only process tasks with context DUMMY_CONTEXT_FOR_LOCAL_TASKS.
 *
 * @author Marc Gathier
 * @since 4.3
 */
@Component
public class LocalTaskManager implements SmartLifecycle {

    private static final String DUMMY_CONTEXT_FOR_LOCAL_TASKS = "_local";
    private static final long MAX_RETRY_INTERVAL = TimeUnit.MINUTES.toMillis(1);
    private final Logger logger = LoggerFactory.getLogger(LocalTaskManager.class);

    private final ScheduledTaskExecutor taskExecutor;
    private final TaskRepository taskRepository;
    private final TaskPayloadSerializer taskPayloadSerializer;
    private final PlatformTransactionManager platformTransactionManager;
    private final Clock clock;
    private final Map<String, String> pendingTasks = new HashMap<>();
    private boolean running;

    public LocalTaskManager(ScheduledTaskExecutor taskExecutor,
                            TaskRepository taskRepository,
                            TaskPayloadSerializer taskPayloadSerializer,
                            PlatformTransactionManager platformTransactionManager,
                            Clock clock) {
        this.taskExecutor = taskExecutor;
        this.taskRepository = taskRepository;
        this.taskPayloadSerializer = taskPayloadSerializer;
        this.platformTransactionManager = platformTransactionManager;
        this.clock = clock;
    }

    /**
     * Checks if there are tasks available to run. If the current node is not the admin leader it does not perform any
     * actions.
     */
    @Scheduled(fixedDelayString = "${axoniq.axonserver.task-manager-delay:1000}", initialDelayString = "${axoniq.axonserver.task-manager-initial-delay:1000}")
    public void checkForTasks() {
        if (!running) {
            return;
        }
        try {
            Set<String> leaderFor = Collections.singleton(DUMMY_CONTEXT_FOR_LOCAL_TASKS);
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

    private void executeTask(Task task) {
        if (pendingTasks.containsKey(task.getTaskId())) {
            return;
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Execute local task {}: {} planned execution {}ms ago",
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
        new TransactionTemplate(platformTransactionManager).execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(@Nonnull TransactionStatus status) {
                try {
                    if (isTransient(cause)) {
                        logger.warn("{}: Retry task {}: {} {}", task.getContext(),
                                    task.getTaskId(),
                                    task.getTaskExecutor(),
                                    cause.getMessage());
                        task.setTimestamp(newSchedule(task));
                        task.setRetryInterval(Math.min(task.getRetryInterval() * 2,
                                                       MAX_RETRY_INTERVAL));
                    } else {
                        logger.warn("{}: Failed to execute task {}: {}", task.getContext(),
                                    task.getTaskId(),
                                    task.getTaskExecutor(),
                                    cause);
                        task.setTimestamp(clock.millis());
                        task.setStatus(Status.FAILED);
                        task.setRetryInterval(0L);
                    }

                    taskRepository.save(task);
                } catch (Exception ex) {
                    logger.warn("{}: updating task repository failed for task {}:{}", task.getContext(),
                                task.getTaskId(),
                                task.getTaskExecutor(),
                                ex);
                }
                pendingTasks.remove(task.getTaskId());
            }
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
        new TransactionTemplate(platformTransactionManager).execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(@Nonnull TransactionStatus status) {
                taskRepository.deleteById(task.getTaskId());
                pendingTasks.remove(task.getTaskId());
            }
        });
    }


    @Transactional
    public void createLocalTask(String taskHandler, Object payload, Duration delay) {
        Payload serializedPayload = taskPayloadSerializer.serialize(payload);
        ScheduleTask task = ScheduleTask.newBuilder()
                                        .setInstant(System.currentTimeMillis() + delay.toMillis())
                                        .setPayload(SerializedObject.newBuilder()
                                                                    .setData(ByteString.copyFrom(serializedPayload
                                                                                                         .getData()))
                                                                    .setType(serializedPayload.getType())
                                                                    .build())
                                        .setTaskId(UUID.randomUUID().toString())
                                        .setTaskExecutor(taskHandler)
                                        .build();

        taskRepository.save(new Task(DUMMY_CONTEXT_FOR_LOCAL_TASKS, task));
    }

    @Override
    public void start() {
        running = true;
    }

    @Override
    public void stop() {
        logger.info("Stop LocalTaskManager");
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
