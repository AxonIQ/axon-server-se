package io.axoniq.axonserver.cluster.configuration.wait.strategy;

import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.Registration;
import io.axoniq.axonserver.cluster.configuration.WaitStrategy;
import io.axoniq.axonserver.cluster.exception.ServerTooSlowException;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class FastUpdateRound implements WaitStrategy {

    private final Supplier<Long> currentTime;

    private final Supplier<Long> timeout;

    private final WaitStrategy updateRound;

    public FastUpdateRound(RaftGroup raftGroup, Supplier<Long> currentTime,
                           Function<Consumer<Long>, Registration> registerMatchIndexListener) {
        this(currentTime,
             () -> Long.valueOf(raftGroup.raftConfiguration().maxElectionTimeout()),
             new UpdateRound(
                     () -> raftGroup.localLogEntryStore().lastLogIndex(),
                     registerMatchIndexListener
             ));
    }

    public FastUpdateRound(Supplier<Long> currentTime, Supplier<Long> timeout, WaitStrategy updateRound) {
        this.currentTime = currentTime;
        this.timeout = timeout;
        this.updateRound = updateRound;
    }

    @Override
    public CompletableFuture<Void> await() {
        long startTime = currentTime.get();

        CompletableFuture<Void> roundCompleted = updateRound.await();

        return roundCompleted.thenRun(() -> {
            long roundDuration = currentTime.get() - startTime;
            long maxRoundDuration = timeout.get();
            if (roundDuration > maxRoundDuration) {
                String message = String.format("The server takes %d to complete the round", roundDuration);
                throw new ServerTooSlowException(message);
            }
        });
    }
}
