package io.axoniq.axonserver.enterprise.component.connection.rule;

import io.axoniq.axonserver.enterprise.component.connection.ConnectionProvider;
import io.axoniq.axonserver.message.ClientIdentification;

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
     * Returns the node with the most convenient {@link ConnectionValue}.
     *
     * @param client the client identifier
     * @param nodes the active nodes in the cluster
     * @return the node with the most convenient {@link ConnectionValue}
     */
    @Override
    public String bestMatch(ClientIdentification client, Iterable<String> nodes) {
        if (client == null) {
            throw new IllegalArgumentException("Client cannot be null");
        }

        if (nodes == null) {
            throw new IllegalArgumentException("Nodes cannot be null");
        }
        String best = null;
        for (String node : nodes) {
            if (best == null) {
                best = node;
            } else {
                ConnectionValue bestWeight = this.rule.apply(client, best);
                ConnectionValue nodeWeight = this.rule.apply(client, node);
                if (nodeWeight.compareTo(bestWeight) > 0) {
                    best = node;
                }
            }
        }
        return best;
    }
}
