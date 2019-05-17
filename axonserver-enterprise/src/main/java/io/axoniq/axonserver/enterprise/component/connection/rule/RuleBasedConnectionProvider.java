package io.axoniq.axonserver.enterprise.component.connection.rule;

import io.axoniq.axonserver.enterprise.component.connection.ConnectionProvider;
import io.axoniq.axonserver.message.ClientIdentification;

/**
 * @author Sara Pellegrini
 * @since 4.2
 */
public class RuleBasedConnectionProvider implements ConnectionProvider {

    private final Rule rule;

    public RuleBasedConnectionProvider(Rule rule) {
        this.rule = rule;
    }

    @Override
    public String bestMatch(ClientIdentification client, Iterable<String> nodes) {
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
