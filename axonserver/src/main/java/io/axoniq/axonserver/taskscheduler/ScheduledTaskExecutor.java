/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.taskscheduler;

import java.util.concurrent.CompletableFuture;

/**
 * Defines the interface for a task executor that will run on the context leader.
 *
 * @author Marc Gathier
 * @since 4.3
 */
public interface ScheduledTaskExecutor {

    /**
     * Executes a task. If the task throws a {@link TransientException} the task will be rescheduled. If it throws
     * another exception
     * it will be set to failed and not tried again.
     *
     * @param task the task to execute
     * @return a completable future that completes when the task is executed
     */
    CompletableFuture<Void> executeTask(Task task);
}
