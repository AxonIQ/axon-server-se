package io.axoniq.axonserver.cluster.configuration;

import io.axoniq.axonserver.grpc.cluster.Node;

import java.util.List;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public interface MembersStore {

    List<Node> get();

    void set(List<Node> nodes);

}
