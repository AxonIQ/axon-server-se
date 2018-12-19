package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.Node;

import java.util.List;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public interface CurrentConfiguration {

    List<Node> groupMembers();

    boolean isUncommitted();

    default boolean isEmpty(){
        return groupMembers().isEmpty();
    }

    default int size(){
        return groupMembers().size();
    }

}
