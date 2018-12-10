package io.axoniq.axonserver.cluster.configuration;

import io.axoniq.axonserver.grpc.cluster.Node;

import java.util.Collection;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public interface MembersStore {

    Collection<Node> get();

    void set(Collection<Node> nodes);

}
