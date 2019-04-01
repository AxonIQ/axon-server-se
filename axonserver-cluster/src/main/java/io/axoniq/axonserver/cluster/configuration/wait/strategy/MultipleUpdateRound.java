package io.axoniq.axonserver.cluster.configuration.wait.strategy;

import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.Registration;
import io.axoniq.axonserver.cluster.configuration.WaitStrategy;
import io.axoniq.axonserver.cluster.exception.ServerTooSlowException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Implementation of {@link WaitStrategy} that waits for the server node to be updated in specific number of round.
 *
 * @author Sara Pellegrini
 * @since 4.0
 */
public class MultipleUpdateRound implements WaitStrategy {

    private final Supplier<Integer> maxRounds;

    private final WaitStrategy round;

    /**
     * Creates an instance with the default configuration.
     * The max number of round is defined from raft group properties,
     * and the {@link WaitStrategy} for the single round is implemented by the {@link FastUpdateRound}.
     *
     * @param raftGroup the raftGroup
     * @param currentTime supplier for current milliseconds
     * @param registerMatchIndexListener registration function to monitor matchIndex updates for the node
     */
    public MultipleUpdateRound(RaftGroup raftGroup,
                               Supplier<Long> currentTime,
                               Function<Consumer<Long>, Registration> registerMatchIndexListener) {
        this(() ->raftGroup.raftConfiguration().maxReplicationRound(),
             new FastUpdateRound(raftGroup,
                                 currentTime,
                                 registerMatchIndexListener));
    }


    /**
     * Creates an instance with the specified supplier for the max rounds number, and the {@link WaitStrategy} for a single round.
     * @param maxRounds supplier of the max number of rounds admitted
     * @param round the {@link WaitStrategy} for the single round, that completes exceptionally
     *              with a {@link ServerTooSlowException} when the round takes too long to be completed
     */
    public MultipleUpdateRound(Supplier<Integer> maxRounds, WaitStrategy round) {
        this.maxRounds = maxRounds;
        this.round = round;
    }

    /**
     * Returns a completable future that completes successfully when the server is updated within correct rounds number.
     * If, after all the possible rounds the server is not up to date, the completable future completes exceptionally,
     * with a {@link ServerTooSlowException}
     *
     * @return the completable future
     */
    @Override
    public CompletableFuture<Void> await() {
        return startRound(0);
    }

    private CompletableFuture<Void> startRound(int iteration) {
        if (iteration >= maxRounds.get()) {
            String message = String.format("The node is not updated in %d rounds", maxRounds.get());
            return failedFuture(new ServerTooSlowException(message));
        }

        CompletableFuture<Void> roundCompleted = round.await();

        try {
            roundCompleted.get();
            return completedFuture(null);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof  ServerTooSlowException){
                return startRound(iteration+1);
            }
            return failedFuture(cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return failedFuture(e);
        }
    }

    private CompletableFuture<Void> failedFuture(Throwable error){
        CompletableFuture<Void> future = new CompletableFuture<>();
        future.completeExceptionally(error);
        return future;
    }

}
