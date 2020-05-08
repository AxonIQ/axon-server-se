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

/**
 * Component that reads tasks from the controldb and tries to execute them.
 * It will process tasks with context specified in the constructor.
 *
 * @author Marc Gathier
 * @since 4.4
 */
public class StandaloneTaskManager extends BaseTaskManager {

    private final String context;
    private final TaskPayloadSerializer taskPayloadSerializer;

    /**
     * Instantiates a {@link StandaloneTaskManager}.
     *
     * @param context                    the context for the tasks to be processed by this task manager.
     * @param taskExecutor               component responsible for executing the tasks
     * @param taskRepository             repository of active tasks
     * @param taskPayloadSerializer      serializer to (de-)serialize task contents
     * @param platformTransactionManager transaction manager
     * @param scheduler                  scheduler to schedule the tasks
     * @param clock                      instance of a clock
     */
    public StandaloneTaskManager(String context,
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

    /**
     * Creates a new task to be executed at some instant.
     *
     * @param taskHandler the class name of the component responsible for executng the task
     * @param payload     the payload of the task
     * @param instant     the timestamp when the task must be executed
     * @return a unique reference to the task
     */
    public String createTask(String taskHandler, TaskPayload payload, long instant) {
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

    /**
     * Creates a new task to be executed at some instant.
     *
     * @param taskHandler the class name of the component responsible for executng the task
     * @param payload     the un-serialized payload of the task
     * @param delay       the time to wait before executing the task
     * @return a unique reference to the task
     */
    public void createTask(String taskHandler, Object payload, Duration delay) {
        createTask(taskHandler, taskPayloadSerializer.serialize(payload), clock.millis() + delay.toMillis());
    }

    @Override
    protected CompletableFuture<Void> processResult(String context, String taskId, TaskStatus status, long newSchedule,
                                                    long retry, String message) {

        if (TaskStatus.COMPLETED.equals(status) || TaskStatus.CANCELLED.equals(status)) {
            new TransactionTemplate(platformTransactionManager).execute(s -> {
                taskRepository.findById(taskId).ifPresent(taskRepository::delete);
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

    /**
     * Cancels a scheduled task.
     *
     * @param taskId the reference to the task
     */
    public void cancel(String taskId) {
        new TransactionTemplate(platformTransactionManager).execute(s -> {
            taskRepository.findById(taskId).ifPresent(taskRepository::delete);
            return null;
        });
        unschedule(context, taskId);
    }


}
