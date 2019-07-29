package io.axoniq.axonserver.cluster.configuration.wait.strategy;

import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.configuration.WaitStrategy;
import io.axoniq.axonserver.cluster.exception.ServerTooSlowException;
import reactor.core.publisher.Flux;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Implementation of {@link WaitStrategy} that waits for the server node to be updated in specific number of rounds.
 *
 * @author Sara Pellegrini
 * @since 4.0
 */
public class MultipleUpdateRound implements WaitStrategy {

    private final Supplier<Integer> maxRounds;

    private final WaitStrategy round;

    /**
     * Creates an instance with the default configuration.
     * The max number of rounds is defined from raft group properties,
     * and the {@link WaitStrategy} for the single round is implemented by the {@link FastUpdateRound}.
     *
     * @param raftGroup the raftGroup
     * @param currentTime supplier for current milliseconds
     * @param matchIndexUpdates flux of the updates of the match index for the new node
     */
    public MultipleUpdateRound(RaftGroup raftGroup,
                               Supplier<Long> currentTime,
                               Flux<Long> matchIndexUpdates) {
        this(() ->raftGroup.raftConfiguration().maxReplicationRound(),
             new FastUpdateRound(raftGroup,
                                 currentTime,
                                 matchIndexUpdates));
    }


    /**
     * Creates an instance with the specified supplier for the max number of rounds, and the {@link WaitStrategy} for a single round.
     * @param maxRounds supplier of the max number of rounds admitted
     * @param round the {@link WaitStrategy} for the single round, that completes exceptionally
     *              with a {@link ServerTooSlowException} when the round takes too long to be completed
     */
    public MultipleUpdateRound(Supplier<Integer> maxRounds, WaitStrategy round) {
        this.maxRounds = maxRounds;
        this.round = round;
    }

    /**
     * Returns a completable future that completes successfully when the server is updated within correct number of rounds.
     * If after all the possible rounds the server is not up to date, the completable future completes exceptionally,
     * with a {@link ServerTooSlowException}
     *
     * @return the completable future
     */
    @Override
    public CompletableFuture<Void> await() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        startRound(0, future);
        return future;
    }

    private void startRound(int iteration, CompletableFuture<Void> future) {
        if (iteration >= maxRounds.get()) {
            String message = String.format("The node is not updated in %d rounds", maxRounds.get());
            future.completeExceptionally(new ServerTooSlowException(message));
        }

        CompletableFuture<Void> roundCompleted = round.await();
        roundCompleted.whenComplete((success, error) -> {
            if (error != null) {
                Throwable cause = error.getCause();
                if (cause instanceof ServerTooSlowException || error instanceof ServerTooSlowException) {
                    startRound(iteration + 1, future);
                } else {
                    future.completeExceptionally(error);
                }
            } else {
                future.complete(null);
            }
        });
    }

}
