package io.axoniq.axonserver.topology;

import java.util.ArrayList;
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

    default boolean isActive(AxonServerNode node) {
        return true;
    }

    default Stream<? extends AxonServerNode> messagingNodes() {
        return Stream.of(getMe());
    }


    default List<AxonServerNode> getRemoteConnections() {
        return new ArrayList<>();
    }

    AxonServerNode getMe();

    default Iterable<String> getMyMessagingContextsNames() {
        return getMe().getMessagingContextNames();
    }

    default AxonServerNode findNodeForClient(String clientName, String componentName, String context) {
        return getMe();
    }

    default Iterable<String> getMyStorageContextNames() {
        return getMe().getStorageContextNames();
    }
}
