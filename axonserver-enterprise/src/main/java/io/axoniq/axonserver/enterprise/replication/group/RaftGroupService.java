package io.axoniq.axonserver.enterprise.replication.group;

import com.google.protobuf.GeneratedMessageV3;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.internal.ReplicationGroup;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupConfiguration;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupUpdateConfirmation;

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
     *
     * @param replicationGroup the context where to add the node to
     * @param node             the node to add
     * @return completable future containing the new confirmation information when it completes
     */
    CompletableFuture<ReplicationGroupUpdateConfirmation> addServer(String replicationGroup, Node node);

    CompletableFuture<Void> getStatus(Consumer<ReplicationGroup> replicationGroupConsumer);

    CompletableFuture<ReplicationGroupConfiguration> initReplicationGroup(String replicationGroup,
                                                                          List<Node> nodes);

    /**
     * Deletes a node from a raft group. Returns configuration of the group as defined in the group after completion.
     * The confirmation may contain an error (for instance when another update is in progress).
     *
     * @param replicationGroup the context where to add the node to
     * @param node             the node to add
     * @return completable future containing the new confirmation information when it completes
     */
    CompletableFuture<ReplicationGroupUpdateConfirmation> deleteServer(String replicationGroup, String node);

    default void stepDown(String replicationGroup) {
    }

    CompletableFuture<Void> deleteReplicationGroup(String replicationGroup, boolean preserveEventStore);

    /**
     * Append an entry to the raft log.
     *
     * @param replicationGroup the raft group name
     * @param name             the type of entry
     * @param bytes            the entry data
     * @return completable future that completes when entry is applied on the leader
     */
    CompletableFuture<Void> appendEntry(String replicationGroup, String name, byte[] bytes);

    default CompletableFuture<Void> appendEntry(String replicationGroup, GeneratedMessageV3 value) {
        return appendEntry(replicationGroup, value.getClass().getName(), value.toByteArray());
    }


    /**
     * Returns the current raft group configuration as a {@link ReplicationGroupConfiguration}
     *
     * @param replicationGroup the raft group name
     * @return the completable future containing the current context configuration
     */
    CompletableFuture<ReplicationGroupConfiguration> configuration(String replicationGroup);

    /**
     * Initiates leadership transfer for the specified {@code replicationGroup}.
     * @param replicationGroup the name of the context
     * @return completable future that completes when follower is up-to-date and signalled to start election
     */
    CompletableFuture<Void> transferLeadership(String replicationGroup);

    CompletableFuture<Void> prepareDeleteNodeFromReplicationGroup(String replicationGroup, String node);
}
