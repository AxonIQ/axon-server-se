package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.internal.ProxyCommandHandler;
import io.axoniq.axonserver.enterprise.cluster.internal.ProxyQueryHandler;
import io.axoniq.axonserver.message.ClientIdentification;
import io.axoniq.axonserver.message.command.CommandRegistrationCache;
import io.axoniq.axonserver.message.query.QueryRegistrationCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Marc Gathier
 */
@ConditionalOnProperty(value = "axoniq.axonserver.clients-connection-strategy", havingValue = "subscriptionCount")
@Component("SubscriptionCountBasedNodeSelectionStrategy")
public class SubscriptionCountBasedNodeSelectionStrategy implements NodeSelectionStrategy {
    private final Logger logger = LoggerFactory.getLogger(SubscriptionCountBasedNodeSelectionStrategy.class);
    private final CommandRegistrationCache commandRegistrationCache;
    private final QueryRegistrationCache queryRegistrationCache;
    private final String thisNodeName;

    public SubscriptionCountBasedNodeSelectionStrategy(CommandRegistrationCache commandRegistrationCache, QueryRegistrationCache queryRegistrationCache,
                                                       MessagingPlatformConfiguration configuration) {
        this.commandRegistrationCache = commandRegistrationCache;
        this.queryRegistrationCache = queryRegistrationCache;
        this.thisNodeName = configuration.getName();
    }

    @Override
    public String selectNode(ClientIdentification clientName, String componentName, Collection<String> activeNodes) {
        Map<String, Integer> nodeWeights = new HashMap<>();
        activeNodes.forEach(n -> nodeWeights.put(n, 0));

        calculateWeightsForCommands(clientName, componentName, nodeWeights);
        calculateWeightsForQueries(clientName, componentName, nodeWeights);

        Map.Entry<String, Integer> minValue = nodeWeights.entrySet().stream()
                .min(Comparator.comparing(Map.Entry::getValue)).orElse(null);

        if( minValue == null || minValue.getValue().equals( nodeWeights.get(thisNodeName))) {
            return thisNodeName;
        }
        return minValue.getKey();
    }

    private void calculateWeightsForQueries(ClientIdentification clientName, String componentName, Map<String, Integer> nodeWeights) {
        queryRegistrationCache.getClients().forEach(client -> {
            if( !client.equals(clientName)) {
                List<QueryRegistrationCache.QueryRegistration> queries = queryRegistrationCache.getForClient(client);
                if (queries != null && ! queries.isEmpty()) {
                    QueryRegistrationCache.QueryRegistration registration = queries.get(0);
                    String key = (registration
                            .getQueryHandler() instanceof ProxyQueryHandler) ? ((ProxyQueryHandler) registration
                            .getQueryHandler()).getMessagingServerName() : thisNodeName;

                    int weight = 100 + queries.size() + (contains(queries, componentName) ? 1000 : 0);
                    nodeWeights.computeIfPresent(key, (k, old) -> weight + old);
                }
            }
        });
    }

    private void calculateWeightsForCommands(ClientIdentification clientName, String componentName,
                                             Map<String, Integer> nodeWeights) {
        commandRegistrationCache.getAll().forEach((client, actions) -> {
            if( ! client.getClient().equals(clientName)) {
                int weight = 100 + actions.size() + ((client.getComponentName()!= null
                        && client.getComponentName().equals(componentName)) ? 1000 : 0);
                String key = (client instanceof ProxyCommandHandler) ? client.getMessagingServerName() : thisNodeName;
                nodeWeights.computeIfPresent(key, (k, old) -> weight + old);
            }
        });
    }

    private boolean contains(
            List<QueryRegistrationCache.QueryRegistration> queries, String componentName) {
        return !queries.isEmpty() && queries.get(0).getQueryHandler().getComponentName().equals(componentName);
    }

    @Override
    public boolean canRebalance(ClientIdentification clientName, String componentName, List<String> activeNodes) {
        logger.debug("Trying to rebalance {}/{}, active AxonHub nodes: {}", clientName, componentName, activeNodes);
        String assignedNode = selectNode(clientName, componentName, activeNodes);
        logger.debug("Result for rebalance {}/{}, {}", clientName, componentName, assignedNode);

        return ! assignedNode.equals(thisNodeName);
    }
}
