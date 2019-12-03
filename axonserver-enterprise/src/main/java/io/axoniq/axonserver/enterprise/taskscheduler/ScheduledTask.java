package io.axoniq.axonserver.enterprise.taskscheduler;

import java.util.concurrent.CompletableFuture;

/**
 * Executes a scheduled task. Any task can either implement the executeAsync or the execute operation, depending on its
 * needs.
 *
 * @author Marc Gathier
 * @since 4.3
 */
public interface ScheduledTask {

    /**
     * Runs a scheduled task asynchronously. Default implementation forwards the task to the synchronous runner ({@code
     * execute}).
     *
     * @param payload the payload for the task
     * @return completable future to notify caller on completed
     */
    default CompletableFuture<Void> executeAsync(Object payload) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        try {
            execute(payload);
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
    default void execute(Object payload) {
    }
}
