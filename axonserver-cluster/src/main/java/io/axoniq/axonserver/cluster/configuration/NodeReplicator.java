package io.axoniq.axonserver.cluster.configuration;

import io.axoniq.axonserver.cluster.Disposable;
import reactor.core.publisher.FluxSink;

/**
 * Represents the action to start to replicate the state on a new node.
 * It is used in order to update a new node before to add the node to the cluster, in order to avoid unavailability.
 * It allows to start the replication process monitoring the updates on matchIndex.
 *
 * This is a functional interface whose functional method is {@link #start(FluxSink)}
 *
 * @author Sara Pellegrini
 * @since 4.0
 */
@FunctionalInterface
public interface NodeReplicator {

    /**
     * Starts to replicate the state on a new node.
     *
     * @param matchIndexUpdates the {@link FluxSink} need to emit the updates for node's match index
     *                          or to notify the interruption of the replication process
     * @return a disposable to stop the replication when the node is up to date
     */
    Disposable start(FluxSink<Long> matchIndexUpdates);

}
