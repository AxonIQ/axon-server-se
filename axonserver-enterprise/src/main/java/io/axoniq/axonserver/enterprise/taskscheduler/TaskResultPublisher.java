package io.axoniq.axonserver.enterprise.taskscheduler;

import io.axoniq.axonserver.grpc.TaskStatus;

import java.util.concurrent.CompletableFuture;

/**
 * Defines the interface to distribute the result of a task execution to all nodes in the context for the task.
 *
 * @author Marc Gathier
 * @since 4.3
 */
public interface TaskResultPublisher {

    /**
     * Takes care of publishing an updated status of a task to the nodes in the specified context.
     *
     * @param context     the context to publish the update in
     * @param taskId      the unique id of the task
     * @param status      the new status of the task
     * @param newSchedule new time to execute the task (if status is scheduled)
     * @param retry       updated retry interval
     */
    default CompletableFuture<Void> publishResult(String context, String taskId, TaskStatus status, Long newSchedule,
                                                  long retry) {
        return publishResult(context, taskId, status, newSchedule, retry, null);
    }

    /**
     * Takes care of publishing an updated status of a task to the nodes in the specified context.
     *
     * @param context     the context to publish the update in
     * @param taskId      the unique id of the task
     * @param status      the new status of the task
     * @param newSchedule new time to execute the task (if status is scheduled)
     * @param retry       updated retry interval
     * @param message     message to save with task
     */
    CompletableFuture<Void> publishResult(String context, String taskId, TaskStatus status, Long newSchedule,
                                          long retry,
                                          String message);
}
