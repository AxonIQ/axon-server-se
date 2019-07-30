package io.axoniq.axonserver.cluster.configuration.wait.strategy;

import io.axoniq.axonserver.cluster.configuration.WaitStrategy;
import io.axoniq.axonserver.cluster.exception.ReplicationInterruptedException;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * {@link WaitStrategy} implementation that waits an update round to be completed.
 * A round is completed when the server has reached at least the last log index provided when the round started.
 *
 * @author Sara Pellegrini
 * @since 4.0
 */
public class UpdateRound implements WaitStrategy {

    private final Supplier<Long> lastIndex;

    private final Flux<Long> matchIndexUpdates;

    /**
     * Creates an instance with the specified last log index supplier and function to monitor matchIndex updates.
     *
     * @param lastIndex         the supplier of the current last log index
     * @param matchIndexUpdates flux of the updates of the match index for the new node
     */
    public UpdateRound(Supplier<Long> lastIndex, Flux<Long> matchIndexUpdates) {
        this.lastIndex = lastIndex;
        this.matchIndexUpdates = matchIndexUpdates;
    }

    /**
     * Returns a completable future that completes successfully when the server reached at least the last log index
     * provided at the beginning of the wait. If no progress is detected for a time exceeding 5 seconds,
     * the completable future completes exceptionally, with a {@link ReplicationInterruptedException}
     *
     * @return the completable future
     */
    @Override
    public CompletableFuture<Void> await() {
        long stopRoundAt = lastIndex.get();
        CompletableFuture<Void> roundCompleted = new CompletableFuture<>();

        Disposable disposable = matchIndexUpdates
                .subscribe(matchIndex -> this.onUpdate(matchIndex, stopRoundAt, roundCompleted),
                           error -> this.onError(roundCompleted),
                           () -> this.onError(roundCompleted));


        roundCompleted.thenRun(disposable::dispose);
        return roundCompleted;
    }

    private void onUpdate(long matchIndex, long stopRoundAt, CompletableFuture<Void> roundCompleted) {
        if (matchIndex >= stopRoundAt) {
            roundCompleted.complete(null);
        }
    }

    private void onError(CompletableFuture<Void> roundCompleted) {
        roundCompleted.completeExceptionally(
                new ReplicationInterruptedException(
                        "The first replication is no longer active. It's likely caused by a leader change."));
    }
}
