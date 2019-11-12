package io.axoniq.axonserver.enterprise.taskscheduler;

import java.util.concurrent.CompletableFuture;

/**
 * Executes a scheduled task.
 *
 * @author Marc Gathier
 * @since 4.3
 */
public interface ScheduledTask {

    CompletableFuture<Void> execute(Object payload);
}
