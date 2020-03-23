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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import javax.transaction.Transactional;

/**
 * Component that reads tasks from the controldb and tries to execute them.
 * It will only process tasks with context {@code DUMMY_CONTEXT_FOR_LOCAL_TASKS}.
 *
 * @author Marc Gathier
 * @since 4.3
 */
public class LocalTaskManager extends BaseTaskManager {

    private final String context;
    private final TaskPayloadSerializer taskPayloadSerializer;

    public LocalTaskManager(String context,
                            ScheduledTaskExecutor taskExecutor,
                            TaskRepository taskRepository,
                            TaskPayloadSerializer taskPayloadSerializer,
                            PlatformTransactionManager platformTransactionManager,
                            @Qualifier("taskScheduler") ScheduledExecutorService scheduler,
                            Clock clock) {
        super(taskExecutor, taskRepository, () -> Collections.singleton(context),
              context::equals,
              platformTransactionManager, scheduler, clock);
        this.context = context;
        this.taskPayloadSerializer = taskPayloadSerializer;
    }

    public String createLocalTask(String taskHandler, Payload payload, long instant) {
        Task task = new Task();
        task.setTaskId(UUID.randomUUID().toString());
        task.setStatus(TaskStatus.SCHEDULED);
        task.setTimestamp(instant);
        task.setTaskExecutor(taskHandler);
        task.setContext(context);
        task.setPayload(payload);
        super.saveAndSchedule(task);
        return task.getTaskId();
    }

    public void createLocalTask(String taskHandler, Object payload, Duration delay) {
        createLocalTask(taskHandler, taskPayloadSerializer.serialize(payload), clock.millis() + delay.toMillis());
    }

    @Override
    protected CompletableFuture<Void> processResult(String context, String taskId, TaskStatus status, long newSchedule,
                                                    long retry, String message) {

        if (TaskStatus.COMPLETED.equals(status) || TaskStatus.CANCELLED.equals(status)) {
            new TransactionTemplate(platformTransactionManager).execute(s -> {
                taskRepository.deleteById(taskId);
                return null;
            });
        } else {
            taskRepository.findById(taskId).ifPresent(task -> {
                task.setTimestamp(newSchedule);
                task.setRetryInterval(retry);
                task.setStatus(status);
                task.setMessage(message);
                saveAndSchedule(task);
            });
        }

        return CompletableFuture.completedFuture(null);
    }

    @Transactional
    public void cancel(String taskId) {
        new TransactionTemplate(platformTransactionManager).execute(s -> {
            taskRepository.deleteById(taskId);
            return null;
        });
        unschedule(taskId);
    }

    public void reschedule(String taskId, long instant) {
        unschedule(taskId);

        taskRepository.findById(taskId).ifPresent(task -> {
            task.setTimestamp(instant);
            task.setRetryInterval(1000L);
            saveAndSchedule(task);
        });
    }

    private void unschedule(String taskId) {
        ScheduledFuture<?> future = scheduledProcessors.getOrDefault(context, Collections.emptyMap())
                                                       .remove(taskId);
        if (future != null) {
            future.cancel(false);
        }
    }
}
