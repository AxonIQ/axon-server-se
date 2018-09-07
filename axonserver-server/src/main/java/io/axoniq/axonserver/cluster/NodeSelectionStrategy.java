package io.axoniq.axonserver.cluster;

import java.util.Collection;
import java.util.List;

/**
 * Author: marc
 */
public interface NodeSelectionStrategy {
    String ME = "___ME";
    String selectNode(String clientName, String componentName, Collection<String> activeNodes);

    boolean canRebalance(String clientName, String componentName, List<String> activeNodes);
}
