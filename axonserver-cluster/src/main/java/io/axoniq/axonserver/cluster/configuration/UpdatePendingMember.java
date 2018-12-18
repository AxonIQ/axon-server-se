package io.axoniq.axonserver.cluster.configuration;

import io.axoniq.axonserver.cluster.Disposable;
import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.Registration;
import io.axoniq.axonserver.cluster.configuration.wait.strategy.MultipleUpdateRound;
import io.axoniq.axonserver.grpc.cluster.Node;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class UpdatePendingMember implements Function<Node, CompletableFuture<Void>> {

    private final Function<Node, NodeReplicator> replicatorFactory;

    private final Function<Function<Consumer<Long>, Registration>, WaitStrategy> waitStrategyFactory;

    public UpdatePendingMember(RaftGroup raftGroup, Supplier<Long> currentTime,
                               Function<Node, NodeReplicator> replicatorFactory) {
        this(replicatorFactory, registerCallback -> new MultipleUpdateRound(raftGroup, currentTime, registerCallback));
    }

    public UpdatePendingMember(
            Function<Node, NodeReplicator> replicatorFactory,
            Function<Function<Consumer<Long>, Registration>, WaitStrategy> waitStrategyFactory) {
        this.replicatorFactory = replicatorFactory;
        this.waitStrategyFactory = waitStrategyFactory;
    }

    @Override
    public CompletableFuture<Void> apply(Node node) {
        AtomicLong matchIndex = new AtomicLong();
        List<Consumer<Long>> matchIndexListeners = new LinkedList<>();

        NodeReplicator nodeReplicator = replicatorFactory.apply(node);

        Disposable replication = nodeReplicator.start(newMatchIndex -> {
            matchIndex.set(newMatchIndex);
            matchIndexListeners.forEach(listener -> listener.accept(newMatchIndex));
        });


        WaitStrategy waitUpdatedStrategy = waitStrategyFactory.apply(
                consumer -> {
                    matchIndexListeners.add(consumer);
                    consumer.accept(matchIndex.get());
                    return () -> matchIndexListeners.remove(consumer);
                });

        return waitUpdatedStrategy.await().whenComplete((success, error) -> {
            replication.dispose();
        });
    }
}
