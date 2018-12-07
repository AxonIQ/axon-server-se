package io.axoniq.axonserver.cluster.configuration.wait.strategy;

import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.Registration;
import io.axoniq.axonserver.cluster.configuration.WaitStrategy;
import io.axoniq.axonserver.cluster.exception.ServerTooSlowException;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class MultipleUpdateRound implements WaitStrategy {

    private final Supplier<Integer> maxRounds;

    private final WaitStrategy round;

    public MultipleUpdateRound(RaftGroup raftGroup,
                               Supplier<Long> currentTime,
                               Function<Consumer<Long>, Registration> registerMatchIndexListener) {
        this(() ->raftGroup.raftConfiguration().maxReplicationRound(),
             new FastUpdateRound(raftGroup,
                                 currentTime,
                                 registerMatchIndexListener));
    }


    public MultipleUpdateRound(Supplier<Integer> maxRounds, WaitStrategy round) {
        this.maxRounds = maxRounds;
        this.round = round;
    }

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
        } catch (Exception e) {
            return startRound(iteration+1);
        }
    }

    private CompletableFuture<Void> failedFuture(Throwable error){
        CompletableFuture<Void> future = new CompletableFuture<>();
        future.completeExceptionally(error);
        return future;
    }

}
