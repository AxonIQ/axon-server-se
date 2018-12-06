package io.axoniq.axonserver.cluster.configuration;

import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.exception.ServerTooSlowException;
import io.axoniq.axonserver.grpc.cluster.Node;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class RoundUpdateAlgorithm implements Function<Node,CompletableFuture<Void>> {

    private final int maxRounds;
    private final long maxLastRoundDuration;
    private final Supplier<Long> currentTime;
    private final Supplier<Long> lastIndex;
    private final BiFunction<Node, Long, CompletableFuture<Void>> matchIndexCaughtUp;

    public RoundUpdateAlgorithm(RaftGroup raftGroup,
                                Supplier<Long> currentTime,
                                BiFunction<Node, Long, CompletableFuture<Void>> matchIndexCaughtUp) {
        this(raftGroup.raftConfiguration().maxReplicationRound(),
             raftGroup.raftConfiguration().maxElectionTimeout(),
             () -> raftGroup.localLogEntryStore().lastLogIndex(),
             currentTime,
             matchIndexCaughtUp);
    }

    public RoundUpdateAlgorithm(int maxRounds,
                                long maxLastRoundDuration,
                                Supplier<Long> lastIndex,
                                Supplier<Long> currentTime,
                                BiFunction<Node, Long, CompletableFuture<Void>> matchIndexCaughtUp) {
        this.maxRounds = maxRounds;
        this.maxLastRoundDuration = maxLastRoundDuration;
        this.currentTime = currentTime;
        this.lastIndex = lastIndex;
        this.matchIndexCaughtUp = matchIndexCaughtUp;
    }


    @Override
    public CompletableFuture<Void> apply(Node node) {
        return startRound(0, node);
    }

    private CompletableFuture<Void> startRound(int iteration, Node node) {
        if (iteration >= maxRounds) {
            throw new ServerTooSlowException(String.format("The node is not updated in %d rounds", maxRounds));
        }

        long startTime = currentTime.get();
        long stopRoundAt = lastIndex.get();

        CompletableFuture<Void> roundCompleted = matchIndexCaughtUp.apply(node, stopRoundAt);

        return roundCompleted.thenCompose(success -> {
            long roundDuration = currentTime.get() - startTime;
            if (roundDuration > maxLastRoundDuration) {
                return startRound(iteration + 1, node);
            }
            return completedFuture(success);
        });

    }

}
