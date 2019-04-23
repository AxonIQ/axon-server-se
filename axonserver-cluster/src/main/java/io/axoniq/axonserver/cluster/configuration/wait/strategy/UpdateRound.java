package io.axoniq.axonserver.cluster.configuration.wait.strategy;

import io.axoniq.axonserver.cluster.Registration;
import io.axoniq.axonserver.cluster.configuration.WaitStrategy;
import io.axoniq.axonserver.cluster.exception.ReplicationTimeoutException;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
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

    private final Function<Consumer<Long>, Registration> registerMatchIndexListener;

    /**
     * Creates an instance with the specified last log index supplier and function to monitor matchIndex updates.
     *
     * @param lastIndex the supplier of the current last log index
     * @param registerMatchIndexListener  registration function to monitor matchIndex updates for the node
     */
    public UpdateRound(Supplier<Long> lastIndex,
                       Function<Consumer<Long>, Registration> registerMatchIndexListener) {
        this.lastIndex = lastIndex;
        this.registerMatchIndexListener = registerMatchIndexListener;
    }

    /**
     * Returns a completable future that completes successfully when the server reached at least the last log index
     * provided at the beginning of the wait. If no progress is detected for a time exceeding 5 seconds,
     * the completable future completes exceptionally, with a {@link ReplicationTimeoutException}
     *
     * @return the completable future
     */
    @Override
    public CompletableFuture<Void> await() {
        long stopRoundAt = lastIndex.get();
        CompletableFuture<Void> roundCompleted = new CompletableFuture<>();
        Flux<Long> flux = Flux.create(emitter -> {
            Registration registration = registerMatchIndexListener.apply(emitter::next);
            emitter.onDispose(registration::cancel);
        });
        Disposable disposable = flux
                .subscribe(matchIndex -> {
                    if (matchIndex >= stopRoundAt) {
                        roundCompleted.complete(null);
                    }
                }, error -> roundCompleted.completeExceptionally(
                        new ReplicationTimeoutException(
                                "The first replication is no longer active. It's likely caused by a leader change.",
                                error)));
        roundCompleted.thenRun(disposable::dispose);
        return roundCompleted;
    }
}
