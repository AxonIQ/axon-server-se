/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.taskscheduler;

import io.axoniq.axonserver.grpc.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.SmartLifecycle;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Task scheduling component that will execute tasks at a specified moment.
 * Tasks that fail with a {@link TransientException} will automatically be rescheduled, with an exponential backup.
 * When tasks fail with another exception they will not be retried.
 *
 * @author Marc Gathier
 * @since 4.4
 */
public abstract class BaseTaskManager implements SmartLifecycle {

    protected static final long MAX_RETRY_INTERVAL = TimeUnit.MINUTES.toMillis(1);
    protected static final Logger logger = LoggerFactory.getLogger(BaseTaskManager.class);

    protected final ScheduledTaskExecutor taskExecutor;
    protected final TaskRepository taskRepository;
    protected final Supplier<Set<String>> leaderForGroupProvider;
    protected final Predicate<String> raftLeaderTest;
    protected final PlatformTransactionManager platformTransactionManager;
    protected final ScheduledExecutorService scheduler;
    protected final Clock clock;
    protected final Map<String, Map<String, ScheduledFuture<?>>> scheduledProcessors = new ConcurrentHashMap<>();
    protected final AtomicLong nextTimestamp = new AtomicLong();
    private final long window = Duration.ofMinutes(5).toMillis();
    private boolean running;

    /**
     * base constructor.
     *
     * @param taskExecutor               component that will execute the task
     * @param taskRepository             repository of scheduled tasks
     * @param leaderForGroupProvider     provides set of contexts where node is leader
     * @param raftLeaderTest             predicate to check if current node is leader for this context
     * @param platformTransactionManager transaction manager
     * @param scheduler                  scheduler component to schedule tasks
     * @param clock                      a clock instance
     */
    public BaseTaskManager(
            ScheduledTaskExecutor taskExecutor, TaskRepository taskRepository,
            Supplier<Set<String>> leaderForGroupProvider,
            Predicate<String> raftLeaderTest,
            PlatformTransactionManager platformTransactionManager,
            @Qualifier("taskScheduler") ScheduledExecutorService scheduler,
            Clock clock) {
        this.taskExecutor = taskExecutor;
        this.taskRepository = taskRepository;
        this.leaderForGroupProvider = leaderForGroupProvider;
        this.raftLeaderTest = raftLeaderTest;
        this.platformTransactionManager = platformTransactionManager;
        this.scheduler = scheduler;
        this.clock = clock;
    }

    protected void saveAndSchedule(Task task) {
        new TransactionTemplate(platformTransactionManager).execute(status -> taskRepository.save(task));
        logger.debug("{}: Task scheduled {}", task.getContext(), task.getTaskId());
        doScheduleTask(task);
    }

    protected void doScheduleTask(Task task) {
        if (raftLeaderTest.test(task.getContext()) &&
                task.getTimestamp() < nextTimestamp.get() &&
                TaskStatus.SCHEDULED.equals(task.getStatus())) {
            logger.debug("{}: adding task to scheduler time {} before {}",
                         task.getContext(),
                         task.getTimestamp(),
                         nextTimestamp);
            schedule(task);
        }
    }

    protected void schedule(Task task) {
        scheduledProcessors.computeIfAbsent(task.getContext(), c -> new ConcurrentHashMap<>())
                           .computeIfAbsent(task.getTaskId(), t ->
                                   scheduler.schedule(() -> executeTask(task),
                                                      delay(task),
                                                      TimeUnit.MILLISECONDS));
    }


    /**
     * Starts the task manager. Loads tasks to execute in near future from task repository and adds them to the
     * scheduler.
     */
    @Override
    public void start() {
        initFetchTasksRunner();
        running = true;
    }

    /**
     * Stops the task manager. Shuts down the scheduler to cancel all scheduled tasks.
     */
    @Override
    public void stop() {
        running = false;
        logger.info("Stop TaskManager");
        scheduler.shutdown();
    }

    /**
     * Returns true if the component is started (and not stopped).
     *
     * @return true if the component is started (and not stopped)
     */
    @Override
    public boolean isRunning() {
        return running;
    }

    /**
     * Updates the status of a task after the task has executed.
     * Reschedules the task if the {@code status} is {@link TaskStatus}.SCHEDULED.
     * Implementations of this method may be async, therefore returning a {@link CompletableFuture}.
     *
     * @param context       the context for the task
     * @param taskId        the unique identification of the task
     * @param status        the new status of the task
     * @param newSchedule   timestamp when the task should be run again (if new status is scheduled)
     * @param retryInterval new retry interval for the task
     * @param message       message from last task execution
     * @return completable future that completes when processing of the result is completed
     */
    protected abstract CompletableFuture<Void> processResult(String context, String taskId, TaskStatus status,
                                                             long newSchedule, long retryInterval, String message);


    private void initFetchTasksRunner() {
        logger.debug("Init fetchTaskRunner, window = {}", window);
        scheduler.scheduleWithFixedDelay(this::fetchTasks, 0, window, TimeUnit.MILLISECONDS);
    }

    private void fetchTasks() {
        try {
            long min = nextTimestamp.getAndSet(clock.millis() + window);
            Set<String> leaderFor = leaderForGroupProvider.get();
            leaderFor.forEach(context -> {
                List<Task> tasks = taskRepository.findScheduled(context, min, nextTimestamp.get());
                logger.debug("{}: scheduling {} tasks between {} and {}",
                             context,
                             tasks.size(),
                             min,
                             nextTimestamp.get());
                tasks.forEach(this::schedule);
            });
        } catch (Exception ex) {
            logger.warn("Exception fetching scheduled tasks, will try again later", ex);
        }
    }

    private void executeTask(Task task) {
        scheduledProcessors.getOrDefault(task.getContext(), Collections.emptyMap()).remove(task.getTaskId());
        if (logger.isDebugEnabled()) {
            logger.debug("{}: Execute task {}: {} planned execution {}ms ago",
                         task.getContext(),
                         task.getTaskId(),
                         task.getTaskExecutor(),
                         clock.millis() - task.getTimestamp()
            );
        }

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

    protected String asString(Throwable cause) {
        if (cause instanceof ExecutionException) {
            return cause.getCause().toString();
        }
        return cause.toString();
    }

    protected boolean isTransient(Throwable cause) {
        if (cause == null) {
            return false;
        }
        if (cause instanceof TransientException) {
            return true;
        }
        return isTransient(cause.getCause());
    }

    private long delay(Task task) {
        return task.getTimestamp() - clock.millis();
    }

    protected long newSchedule(Task task) {
        return clock.millis() + Math.min(task.getRetryInterval(), MAX_RETRY_INTERVAL);
    }

    protected void error(Task task, Throwable cause) {
        CompletableFuture<Void> publishResultFuture;
        if (isTransient(cause)) {

            long retryInterval = Math.min(task.getRetryInterval() * 2,
                    MAX_RETRY_INTERVAL);

            if(task.getRetryInterval() < MAX_RETRY_INTERVAL) {
                logger.info("{}: Failed to execute task '{}'.  Retrying in {} ms...",
                        task.getContext(),
                        task.getPayload().getType(),
                        retryInterval);
            } else {
                logger.warn("{}: Failed to execute task {}: {} - {}. Retrying in {} ms...",
                        task.getContext(),
                        task.getTaskId(),
                        task.getTaskExecutor(),
                        cause.getMessage(),
                        retryInterval);
            }

            publishResultFuture = processResult(task.getContext(),
                                                task.getTaskId(),
                                                TaskStatus.SCHEDULED,
                                                newSchedule(task),
                    retryInterval, asString(cause));
        } else {
            logger.warn("{}: Failed to execute task {}: {}", task.getContext(),
                        task.getTaskId(),
                        task.getTaskExecutor(),
                        cause);
            publishResultFuture = processResult(task.getContext(),
                                                task.getTaskId(),
                                                TaskStatus.FAILED,
                                                clock.millis(),
                                                0, asString(cause));
        }

        publishResultFuture.exceptionally(e -> {
            logger.warn("{}: Failed to process result for task {}: {}", task.getContext(),
                        task.getTaskId(),
                        task.getTaskExecutor(),
                        e);
            return null;
        });
    }

    protected void completed(Task task) {
        processResult(task.getContext(),
                      task.getTaskId(),
                      TaskStatus.COMPLETED,
                      clock.millis(),
                      0, null)
                .exceptionally(e -> {
                    logger.warn("{}: Failed to process result for completed task {}: {}", task.getContext(),
                                task.getTaskId(),
                                task.getTaskExecutor(),
                                e);
                    return null;
                });
    }

    protected void unschedule(String context, String taskId) {
        ScheduledFuture<?> future = scheduledProcessors.getOrDefault(context, Collections.emptyMap())
                                                       .remove(taskId);
        if (future != null) {
            future.cancel(false);
        }
    }
}
