package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.message.ClientIdentification;

import java.util.Collection;
import java.util.List;

/**
 * @author Marc Gathier
 */
public interface NodeSelectionStrategy {
    String selectNode(ClientIdentification clientName, String componentName, Collection<String> activeNodes);

    boolean canRebalance(ClientIdentification clientName, String componentName, List<String> activeNodes);
}
