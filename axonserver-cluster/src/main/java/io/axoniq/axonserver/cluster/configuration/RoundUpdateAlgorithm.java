package io.axoniq.axonserver.cluster.configuration;

import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.Registration;
import io.axoniq.axonserver.cluster.exception.ServerTooSlowException;
import io.axoniq.axonserver.grpc.cluster.Node;

import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class RoundUpdateAlgorithm implements Consumer<Node> {

    private final int maxRounds;
    private final long maxLastRoundDuration;
    private final Supplier<Long> currentTime;
    private final Supplier<Long> lastIndex;
    private final BiFunction<Node, Consumer<Long>, Registration> registerMatchIndexListener;

    public RoundUpdateAlgorithm(RaftGroup raftGroup,
                                Supplier<Long> currentTime,
                                BiFunction<Node, Consumer<Long>, Registration> registerMatchIndexListener) {
        this(raftGroup.raftConfiguration().maxReplicationRound(),
             raftGroup.raftConfiguration().maxElectionTimeout(),
             () -> raftGroup.localLogEntryStore().lastLogIndex(),
             currentTime,
             registerMatchIndexListener);
    }

    public RoundUpdateAlgorithm(int maxRounds,
                                long maxLastRoundDuration,
                                Supplier<Long> lastIndex,
                                Supplier<Long> currentTime,
                                BiFunction<Node, Consumer<Long>, Registration> registerMatchIndexListener) {
        this.maxRounds = maxRounds;
        this.maxLastRoundDuration = maxLastRoundDuration;
        this.currentTime = currentTime;
        this.lastIndex = lastIndex;
        this.registerMatchIndexListener = registerMatchIndexListener;
    }


    @Override
    public void accept(Node node) {
        startRound(0, node);
    }

    private void startRound(int iteration, Node node) {
        if (iteration >= maxRounds) {
            throw new ServerTooSlowException(String.format("The node is not updated in %d rounds", maxRounds));
        }

        long startTime = currentTime.get();
        long stopRoundAt = lastIndex.get();

        CountDownLatch roundCompleted = new CountDownLatch(1);
        Registration registration = registerMatchIndexListener.apply(node, matchIndex -> {
            if (matchIndex >= stopRoundAt) {
                roundCompleted.countDown();
            }
        });

        try {
            roundCompleted.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Thread interrupted while waiting the update of node " + node.getNodeId());
        }
        registration.cancel();

        long roundDuration = currentTime.get() - startTime;
        if (roundDuration > maxLastRoundDuration) {
            startRound(iteration + 1, node);
        }
    }
}
