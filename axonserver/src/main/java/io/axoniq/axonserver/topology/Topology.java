package io.axoniq.axonserver.topology;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * @author Marc Gathier
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

    default Stream<? extends AxonServerNode> nodes() {
        return Stream.of(getMe());
    }

    default List<AxonServerNode> getRemoteConnections() {
        return new ArrayList<>();
    }

    AxonServerNode getMe();

    default AxonServerNode findNodeForClient(String clientName, String componentName, String context) {
        return getMe();
    }

    default Iterable<String> getMyContextNames() {
        return getMe().getContextNames();
    }
}
