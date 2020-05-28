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

import java.util.Optional;
import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * Scheduled task to be run by the admin leader.
 *
 * @author Marc Gathier
 * @since 4.4
 */
@Entity
public class Task {

    @Id
    private String taskId;

    private String taskExecutor;

    private TaskPayload payload;

    private Long timestamp;

    private TaskStatus status;

    private Long retryInterval;

    private String context;
    private String message;

    public Task() {
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getTaskExecutor() {
        return taskExecutor;
    }

    public void setTaskExecutor(String taskExecutor) {
        this.taskExecutor = taskExecutor;
    }

    public TaskPayload getPayload() {
        return payload;
    }

    public void setPayload(TaskPayload payload) {
        this.payload = payload;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public TaskStatus getStatus() {
        return status;
    }

    public void setStatus(TaskStatus status) {
        this.status = status;
    }

    public Long getRetryInterval() {
        return Optional.ofNullable(retryInterval).orElse(1000L);
    }

    public void setRetryInterval(Long retryInterval) {
        this.retryInterval = retryInterval;
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public void setMessage(String message) {
        this.message = trim(message, 255);
    }

    private String trim(String message, int maxLength) {
        if (message == null || message.length() <= maxLength) {
            return message;
        }

        return message.substring(0, maxLength);
    }

    public String getMessage() {
        return message;
    }
}
