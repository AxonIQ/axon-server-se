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
 * Executes a scheduled task. Any task can either implement the executeAsync or the execute operation, depending on its
 * needs.
 *
 * @author Marc Gathier
 * @since 4.4
 */
public interface ScheduledTask {

    /**
     * Runs a scheduled task asynchronously. Default implementation forwards the task to the synchronous runner ({@code
     * execute}).
     *
     * @param payload the payload for the task
     * @return completable future to notify caller on completed
     */
    default CompletableFuture<Void> executeAsync(String context, Object payload) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        try {
            execute(context, payload);
            completableFuture.complete(null);
        } catch (Exception ex) {
            completableFuture.completeExceptionally(ex);
        }
        return completableFuture;
    }

    /**
     * Runs a scheduled task synchronously. Default implementation is a no-op.
     *
     * @param payload the payload for the task
     */
    default void execute(String context, Object payload) {
    }

    /**
     * Returns true if the payload for tasks of this type is json serialized.
     *
     * @return true if the payload for tasks of this type is json serialized
     */
    default boolean isSerialized() {
        return true;
    }
}
