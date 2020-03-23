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
 * Repository of tasks to execute on the admin leader.
 *
 * @author Marc Gathier
 * @since 4.3
 */
public interface TaskRepository extends JpaRepository<Task, String> {

    /**
     * Finds all tasks for a specified context
     *
     * @param context the context name
     * @return list of tasks for the specified context
     */
    List<Task> findAllByContext(String context);

    Iterable<Task> findAllByContextAndStatusAndTimestampGreaterThanEqualAndTimestampLessThan(
            String context, TaskStatus status, long minTimestamp, long maxTimestamp);

    /**
     * Deletes all tasks for a specified context
     *
     * @param context the context name
     */
    void deleteAllByContext(String context);

    default Iterable<Task> findScheduled(String context, long minTimestamp, long maxTimestamp) {
        return findAllByContextAndStatusAndTimestampGreaterThanEqualAndTimestampLessThan(context,
                                                                                         TaskStatus.SCHEDULED,
                                                                                         minTimestamp,
                                                                                         maxTimestamp);
    }
}
