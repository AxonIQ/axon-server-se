package io.axoniq.axonserver.enterprise.taskscheduler;

import io.axoniq.axonserver.grpc.tasks.Status;

import java.util.concurrent.CompletableFuture;

/**
 * Defines the interface to distribute the result of a task execution to all admin nodes.
 * @author Marc Gathier
 * @since 4.3
 */
public interface TaskResultPublisher {

    /**
     * Takes care of publishing an updated status of a task to the nodes in the specified context.
     *
     * @param context      the context to publish the update in
     * @param taskId      the unique id of the task
     * @param status      the new status of the task
     * @param newSchedule new time to execute the task (if status is scheduled)
     * @param retry       updated retry interval
     */
    CompletableFuture<Void> publishResult(String context, String taskId, Status status, Long newSchedule, long retry);
}
