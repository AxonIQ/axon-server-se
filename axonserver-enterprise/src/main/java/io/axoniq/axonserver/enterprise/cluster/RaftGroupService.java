package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.internal.Context;
import io.axoniq.axonserver.grpc.internal.ContextApplication;
import io.axoniq.axonserver.grpc.internal.ContextConfiguration;
import io.axoniq.axonserver.grpc.internal.ContextUpdateConfirmation;
import io.axoniq.axonserver.grpc.internal.ContextUser;
import io.axoniq.axonserver.grpc.internal.LoadBalanceStrategy;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Interface to perform actions on the leader of a specific raft group
 * @author Marc Gathier
 */
public interface RaftGroupService {

    /**
     * Adds a node to a raft group. Returns configuration of the group as defined in the group after completion.
     * The confirmation may contain an error (for instance when another update is in progress).
     * @param context the context where to add the node to
     * @param node the node to add
     * @return completable future containing the new confirmation information when it completes
     */
    CompletableFuture<ContextUpdateConfirmation> addNodeToContext(String context, Node node);

    CompletableFuture<Void> getStatus(Consumer<Context> contextConsumer);

    CompletableFuture<ContextConfiguration> initContext(String context, List<Node> nodes);

    /**
     * Deletes a node from a raft group. Returns configuration of the group as defined in the group after completion.
     * The confirmation may contain an error (for instance when another update is in progress).
     * @param context the context where to add the node to
     * @param node the node to add
     * @return completable future containing the new confirmation information when it completes
     */
    CompletableFuture<ContextUpdateConfirmation> deleteNode(String context, String node);

    default void stepDown(String context) {
    }

    CompletableFuture<Void> updateApplication(ContextApplication application);

    CompletableFuture<Void> deleteApplication(ContextApplication application);

    CompletableFuture<Void> updateUser(ContextUser user);

    CompletableFuture<Void> deleteUser(ContextUser user);

    CompletableFuture<Void> updateLoadBalancingStrategy(String context, LoadBalanceStrategy loadBalancingStrategy);

    CompletableFuture<Void> updateProcessorLoadBalancing(String context, ProcessorLBStrategy processorLBStrategy);

    CompletableFuture<Void> deleteLoadBalancingStrategy(String context, LoadBalanceStrategy loadBalancingStrategy);

    CompletableFuture<Void> deleteContext(String context);

    /**
     * Append an entry to the raft log.
     * @param context the raft group name
     * @param name the type of entry
     * @param bytes the entry data
     * @return completable future that completes when entry is applied on the leader
     */
    CompletableFuture<Void> appendEntry(String context, String name, byte[] bytes);

    /**
     * Returns the current raft group configuration as a {@link ContextConfiguration}
     * @param context the raft group name
     * @return the completable future containing the current context configuration
     */
    CompletableFuture<ContextConfiguration> configuration(String context);

    /**
     * Initiates leadership transfer for the specified {@code context}.
     * @param context the name of the context
     * @return completable future that completes when follower is up-to-date and signalled to start election
     */
    CompletableFuture<Void> transferLeadership(String context);
}
