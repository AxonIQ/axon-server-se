package io.axoniq.axonserver.enterprise.taskscheduler;

import io.axoniq.axonserver.enterprise.jpa.Task;

import java.util.concurrent.CompletableFuture;

/**
 * Defines the interface for a task executor that will run on the context leader.
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
