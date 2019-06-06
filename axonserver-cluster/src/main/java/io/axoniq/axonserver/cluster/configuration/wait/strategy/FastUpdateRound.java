package io.axoniq.axonserver.cluster.configuration.wait.strategy;

import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.configuration.WaitStrategy;
import io.axoniq.axonserver.cluster.exception.ServerTooSlowException;
import reactor.core.publisher.Flux;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Implementation of {@link WaitStrategy} that waits for the server node to complete a round within a specific timeout.
 *
 * @author Sara Pellegrini
 * @since 4.0
 */
public class FastUpdateRound implements WaitStrategy {

    private final Supplier<Long> currentTime;

    private final Supplier<Long> timeout;

    private final WaitStrategy updateRound;

    /**
     * Creates an instance with the default configuration.
     * The max timeout for the round to be completed is defined from raft configuration {@code maxElectionTimeout}
     * property, and the {@link WaitStrategy} for the round completion is implemented by the {@link UpdateRound}.
     *
     * @param raftGroup         the raftGroup
     * @param currentTime       supplier for current milliseconds
     * @param matchIndexUpdates flux of the updates of the match index for the new node
     */
    public FastUpdateRound(RaftGroup raftGroup, Supplier<Long> currentTime, Flux<Long> matchIndexUpdates) {
        this(currentTime,
             () -> Long.valueOf(raftGroup.raftConfiguration().maxElectionTimeout()),
             new UpdateRound(() -> raftGroup.localLogEntryStore().lastLogIndex(), matchIndexUpdates));
    }

    /**
     * Creates an instance with the specified suppliers for the currentTime and the timeout,
     * and the {@link WaitStrategy} for a simple round.
     *
     * @param currentTime supplier of the current time
     * @param currentTime supplier of the max timeout admitted for round completion
     * @param updateRound the {@link WaitStrategy} for the round to be completed
     */
    public FastUpdateRound(Supplier<Long> currentTime, Supplier<Long> timeout, WaitStrategy updateRound) {
        this.currentTime = currentTime;
        this.timeout = timeout;
        this.updateRound = updateRound;
    }

    /**
     * Returns a completable future that completes successfully when the update round is completed within the timeout.
     * If the update round completion time exceeds the timeout, the completable future completes exceptionally with a
     * {@link ServerTooSlowException}
     *
     * @return the completable future
     */
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
