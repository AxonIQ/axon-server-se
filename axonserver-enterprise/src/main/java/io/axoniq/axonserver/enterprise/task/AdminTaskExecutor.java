package io.axoniq.axonserver.enterprise.task;

import io.axoniq.axonserver.enterprise.jpa.Task;

/**
 * Defines the interface for a task executor that will run on the admin leader.
 * @author Marc Gathier
 * @since 4.3
 */
public interface AdminTaskExecutor {

    /**
     * Executes a task. If the task throws a {@link TransientException} the task will be rescheduled. If it throws
     * another exception
     * it will be set to failed and not tried again.
     *
     * @param task the task to execute
     */
    void executeTask(Task task);
}
