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

    /**
     * Gets the names of all contexts where the current Axon Server instance is member of. In Axon Server Standard this only contains DEFAULT_CONTEXT, in
     * Axon Server Enterprise this is dynamic.
     *
     * @return names of contexts
     */
    default Iterable<String> getMyContextNames() {
        return getMe().getContextNames();
    }

    /**
     * Checks if this node serves as administrative node for Axon Server. Always true for Standard Edition.
     *
     * @return true if this node is an administative node
     */
    default boolean isAdminNode() {
        return true;
    }

    /**
     * Gets the names of all contexts where the current Axon Server instance is member of, and it is storing events for.
     * In Axon Server Standard this only contains DEFAULT_CONTEXT, in Axon Server Enterprise this is dynamic.
     *
     * @return names of contexts
     */
    default Iterable<String> getMyStorageContextNames() {
        return getMyContextNames();
    }
}
