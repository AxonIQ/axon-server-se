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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
 * @author Marc Gathier
 */
public abstract class BaseTaskManager implements SmartLifecycle {

    protected static final long MAX_RETRY_INTERVAL = TimeUnit.MINUTES.toMillis(1);

    protected final Logger logger = LoggerFactory.getLogger(BaseTaskManager.class);
    protected final ScheduledTaskExecutor taskExecutor;
    protected final TaskRepository taskRepository;
    protected final Supplier<Set<String>> raftGroupProvider;
    protected final Predicate<String> raftLeaderTest;
    protected final PlatformTransactionManager platformTransactionManager;
    protected final ScheduledExecutorService scheduler;
    protected final Clock clock;
    protected final Map<String, Map<String, ScheduledFuture>> scheduledProcessors = new ConcurrentHashMap<>();
    protected final AtomicLong nextTimestamp = new AtomicLong();
    private final long window = Duration.ofMinutes(5).toMillis();
    private boolean running;

    public BaseTaskManager(
            ScheduledTaskExecutor taskExecutor, TaskRepository taskRepository,
            Supplier<Set<String>> raftGroupProvider,
            Predicate<String> raftLeaderTest,
            PlatformTransactionManager platformTransactionManager,
            @Qualifier("taskScheduler") ScheduledExecutorService scheduler,
            Clock clock) {
        this.taskExecutor = taskExecutor;
        this.taskRepository = taskRepository;
        this.raftGroupProvider = raftGroupProvider;
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


    @Override
    public void start() {
        initFetchTasksRunner();
        running = true;
    }

    @Override
    public void stop() {
        logger.info("Stop TaskManager");
        scheduler.shutdown();
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private void initFetchTasksRunner() {
        logger.debug("Init fetchTaskRunner, window = {}", window);
        nextTimestamp.set(clock.millis() + window);
        scheduler.scheduleWithFixedDelay(this::fetchTasks, 0, window, TimeUnit.MILLISECONDS);
    }

    private void fetchTasks() {
        try {
            long min = nextTimestamp.getAndAdd(window);
            if (min == 0) {
                nextTimestamp.getAndAdd(window);
            }
            Set<String> leaderFor = raftGroupProvider.get();
            leaderFor.forEach(context -> {
                Iterable<Task> tasks = taskRepository.findScheduled(context, min, nextTimestamp.get());
                logger.trace("{}: scheduling more tasks {} between {} and {}",
                             context,
                             ((List) tasks).size(),
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
            logger.warn("{}: Failed to execute task {}: {} - {}", task.getContext(),
                        task.getTaskId(),
                        task.getTaskExecutor(),
                        cause.getMessage());
            publishResultFuture = processResult(task.getContext(),
                                                task.getTaskId(),
                                                TaskStatus.SCHEDULED,
                                                newSchedule(task),
                                                Math.min(task.getRetryInterval() * 2,
                                                         MAX_RETRY_INTERVAL), asString(cause));
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
            logger.warn("{}: Failed to publish result for task {}: {}", task.getContext(),
                        task.getTaskId(),
                        task.getTaskExecutor(),
                        e);
            return null;
        });
    }

    protected abstract CompletableFuture<Void> processResult(String context, String taskId, TaskStatus scheduled,
                                                             long newSchedule, long min, String asString);


    protected void completed(Task task) {
        processResult(task.getContext(),
                      task.getTaskId(),
                      TaskStatus.COMPLETED,
                      clock.millis(),
                      0, null)
                .exceptionally(e -> {
                    logger.warn("{}: Failed to publish result for completed task {}: {}", task.getContext(),
                                task.getTaskId(),
                                task.getTaskExecutor(),
                                e);
                    return null;
                });
    }
}
