package io.axoniq.axonserver.enterprise.component.connection.rule;

import io.axoniq.axonserver.enterprise.component.connection.ConnectionProvider;
import io.axoniq.axonserver.message.ClientIdentification;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Implementation of {@link ConnectionProvider} that chooses the best connection based on the specified {@link Rule}.
 *
 * @author Sara Pellegrini
 * @since 4.2
 */
public class RuleBasedConnectionProvider implements ConnectionProvider {

    private final Rule rule;

    /**
     * Creates an instance that uses the specified {@link Rule} to calculate the {@link ConnectionValue}.
     *
     * @param rule the rule
     */
    public RuleBasedConnectionProvider(Rule rule) {
        this.rule = rule;
    }

    /**
     * Returns the node with the most convenient {@link ConnectionValue}. If multiple nodes have equally suitable
     * {@link ConnectionValue} then all the suitable nodes are returned.
     *
     * @param client the client identifier
     * @param nodes the active nodes in the cluster
     * @return the nodes with the most convenient {@link ConnectionValue}
     */
    @Override
    public List<String> bestMatches(ClientIdentification client, Iterable<String> nodes) {
        if (client == null) {
            throw new IllegalArgumentException("Client cannot be null");
        }

        if (nodes == null) {
            throw new IllegalArgumentException("Nodes cannot be null");
        }

        HashMap<String,Double> nodeWeights = new HashMap<>();
        Double highestValue = 0D;

        for (String node : nodes) {
            Double currentNodeWeight = this.rule.apply(client,node).weight();
            nodeWeights.put(node, currentNodeWeight);
            if (currentNodeWeight > highestValue) {
                highestValue = currentNodeWeight;
            }
        }

        final Double finalHighestValue = highestValue;
        return nodeWeights.entrySet().stream().filter(node -> node.getValue() >= finalHighestValue)
                          .map(Map.Entry::getKey).collect(Collectors.toList());
    }
}
