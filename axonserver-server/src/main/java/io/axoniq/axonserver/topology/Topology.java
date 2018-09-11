package io.axoniq.axonserver.topology;

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

/**
 * Author: marc
 */
public interface Topology {
    String DEFAULT_CONTEXT = "default";

    String getName();

    default boolean isMultiContext() {
        return false;
    }

    boolean isActive(AxonServerNode node);

    Stream<AxonServerNode> messagingNodes();

    List<AxonServerNode> getRemoteConnections();

    AxonServerNode getMe();
}
