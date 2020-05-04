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
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

/**
 * Repository of tasks to execute.
 *
 * @author Marc Gathier
 * @since 4.4
 */
public interface TaskRepository extends JpaRepository<Task, String> {

    /**
     * Finds all tasks for a specified context
     *
     * @param context the context name
     * @return list of tasks for the specified context
     */
    List<Task> findAllByContext(String context);

    /**
     * Finds all tasks for a {@code context} with given {@code status} to be executed within given time period.
     *
     * @param context      the name of the context
     * @param status       the status of the task
     * @param minTimestamp minimum timestamp (exclusive)
     * @param maxTimestamp maximum timestamp (inclusive)
     * @return list of matching tasks
     */
    List<Task> findAllByContextAndStatusAndTimestampGreaterThanEqualAndTimestampLessThan(
            String context, TaskStatus status, long minTimestamp, long maxTimestamp);

    /**
     * Deletes all tasks for a specified context
     *
     * @param context the context name
     */
    void deleteAllByContext(String context);

    /**
     * Finds all scheduled tasks for a {@code context} to be executed within given time period.
     *
     * @param context      the name of the context
     * @param minTimestamp minimum timestamp (exclusive)
     * @param maxTimestamp maximum timestamp (inclusive)
     * @return list of scheduled tasks
     */
    default List<Task> findScheduled(String context, long minTimestamp, long maxTimestamp) {
        return findAllByContextAndStatusAndTimestampGreaterThanEqualAndTimestampLessThan(context,
                                                                                         TaskStatus.SCHEDULED,
                                                                                         minTimestamp,
                                                                                         maxTimestamp);
    }

    List<Task> findAllByContextIn(List<String> context);
}
