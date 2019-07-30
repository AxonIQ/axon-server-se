package io.axoniq.axonserver.cluster.configuration;

import io.axoniq.axonserver.cluster.Disposable;
import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.configuration.wait.strategy.MultipleUpdateRound;
import io.axoniq.axonserver.grpc.cluster.Node;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Represents the update operation needed before to accept a new node as a member of the cluster.
 *
 * @author Sara Pellegrini
 * @since 4.0
 */
public class UpdatePendingMember implements Function<Node, CompletableFuture<Void>> {

    private final Function<Node, NodeReplicator> replicatorFactory;

    private final Function<Flux<Long>, WaitStrategy> waitStrategyFactory;

    /**
     * Creates an instance with the default configuration.
     * The {@link WaitStrategy}'s factory creates {@link MultipleUpdateRound} instances.
     *
     * @param raftGroup         the raft group
     * @param currentTime       supplier for current milliseconds
     * @param replicatorFactory factory of {@link NodeReplicator} needed to update the new node
     */
    public UpdatePendingMember(RaftGroup raftGroup, Supplier<Long> currentTime,
                               Function<Node, NodeReplicator> replicatorFactory) {
        this(replicatorFactory,
             matchIndexUpdates -> new MultipleUpdateRound(raftGroup, currentTime, matchIndexUpdates));
    }


    /**
     * Creates an instance with the specified factories for the {@link NodeReplicator} and {@link WaitStrategy}
     *
     * @param replicatorFactory   factory of {@link NodeReplicator} needed to update the new node
     * @param waitStrategyFactory factory of {@link WaitStrategy} needed to verify if node updates in the correct timing
     */
    public UpdatePendingMember(
            Function<Node, NodeReplicator> replicatorFactory,
            Function<Flux<Long>, WaitStrategy> waitStrategyFactory) {
        this.replicatorFactory = replicatorFactory;
        this.waitStrategyFactory = waitStrategyFactory;
    }

    /**
     * Executes a state replication on the node in order to update its state.
     * Returns a completable future that completes successfully when the update is completed in the correct timing.
     *
     * @param node the node to be updated
     * @return the completable future of the update operation
     */
    @Override
    public CompletableFuture<Void> apply(Node node) {
        NodeReplicator nodeReplicator = replicatorFactory.apply(node);
        EmitterProcessor<Long> matchIndexUpdateEmitter = EmitterProcessor.create(10);
        Disposable replication = nodeReplicator.start(matchIndexUpdateEmitter.sink());
        WaitStrategy waitUpdatedStrategy = waitStrategyFactory.apply(matchIndexUpdateEmitter.replay().autoConnect());

        return waitUpdatedStrategy.await().whenComplete((success, error) -> {
            replication.dispose();
        });
    }
}
