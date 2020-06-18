/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.taskscheduler;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * Component that manages the execution of a scheduled task.
 *
 * @author Marc Gathier
 * @since 4.4
 */
@Service
public class ScheduledTaskExecutorImpl implements ScheduledTaskExecutor {

    private final ApplicationContext applicationContext;
    private final TaskPayloadSerializer taskPayloadSerializer;

    /**
     * Creates an instance.
     * @param applicationContext used to retrieve the bean implementing a task
     * @param taskPayloadSerializer to deserialize the payload of task (if needed)
     */
    public ScheduledTaskExecutorImpl(ApplicationContext applicationContext,
                                     TaskPayloadSerializer taskPayloadSerializer) {
        this.applicationContext = applicationContext;
        this.taskPayloadSerializer = taskPayloadSerializer;
    }


    @Override
    public CompletableFuture<Void> executeTask(Task task) {
        try {
            ScheduledTask job = (ScheduledTask) applicationContext.getBean(Class.forName(task.getTaskExecutor()));
            Object payload = task.getPayload().asSerializedObject();
            if (job.isSerialized()) {
                payload = taskPayloadSerializer.deserialize(task.getPayload());
            }
            return job.executeAsync(task.getContext(), payload);
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException("Could not find handler for task as a Spring bean: " + task.getTaskExecutor(),
                                       ex);
        } catch (BeansException ex) {
            throw new RuntimeException("Could not create handler for task as a Spring bean: " + task.getTaskExecutor(),
                                       ex);
        }
    }
}
