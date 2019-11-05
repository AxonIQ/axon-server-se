package io.axoniq.axonserver.enterprise.task;

import io.axoniq.axonserver.grpc.tasks.Status;

/**
 * Defines the interface to distribute the result of a task execution to all admin nodes.
 * @author Marc Gathier
 * @since 4.3
 */
public interface TaskResultPublisher {

    /**
     * Takes care of publishing an updated status of a task to the admin nodes.
     *
     * @param taskId      the unique id of the task
     * @param status      the new status of the task
     * @param newSchedule new time to execute the task (if status is scheduled)
     * @param retry       updated retry interval
     */
    void publishResult(String taskId, Status status, Long newSchedule, long retry);
}
