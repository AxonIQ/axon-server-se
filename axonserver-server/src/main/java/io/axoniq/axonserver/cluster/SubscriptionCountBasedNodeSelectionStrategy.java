package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.internal.ProxyCommandHandler;
import io.axoniq.axonserver.grpc.internal.ProxyQueryHandler;
import io.axoniq.axonserver.message.command.CommandRegistrationCache;
import io.axoniq.axonserver.message.query.QueryRegistrationCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Author: marc
 */
@Component("SubscriptionCountBasedNodeSelectionStrategy")
public class SubscriptionCountBasedNodeSelectionStrategy implements NodeSelectionStrategy {
    private final Logger logger = LoggerFactory.getLogger(SubscriptionCountBasedNodeSelectionStrategy.class);
    private final CommandRegistrationCache commandRegistrationCache;
    private final QueryRegistrationCache queryRegistrationCache;

    public SubscriptionCountBasedNodeSelectionStrategy(CommandRegistrationCache commandRegistrationCache, QueryRegistrationCache queryRegistrationCache) {
        this.commandRegistrationCache = commandRegistrationCache;
        this.queryRegistrationCache = queryRegistrationCache;
    }

    @Override
    public String selectNode(String clientName, String componentName, Collection<String> activeNodes) {
        Map<String, Integer> nodeWeights = new HashMap<>();
        activeNodes.forEach(n -> nodeWeights.put(n, 0));

        commandRegistrationCache.getAll().forEach((client, actions) -> {
            if( ! client.getClient().equals(clientName)) {
                int weight = 100 + actions.size() + ((client.getComponentName()!= null
                        && client.getComponentName().equals(componentName)) ? 1000 : 0);
                String key = (client instanceof ProxyCommandHandler) ? ((ProxyCommandHandler)client).getMessagingServerName() : ME;
                nodeWeights.computeIfPresent(key, (k, old) -> weight + old);
            }
        });


        queryRegistrationCache.getClients().forEach(client -> {
            if( !client.equals(clientName)) {
                List<QueryRegistrationCache.QueryRegistration> queries = queryRegistrationCache.getForClient(client);
                if (queries != null && queries.size() > 0) {
                    QueryRegistrationCache.QueryRegistration registration = queries.get(0);
                    String key = (registration
                            .getQueryHandler() instanceof ProxyQueryHandler) ? ((ProxyQueryHandler) registration
                            .getQueryHandler()).getMessagingServerName() : ME;

                    int weight = 100 + queries.size() + (contains(queries, componentName) ? 1000 : 0);
                    nodeWeights.computeIfPresent(key, (k, old) -> weight + old);
                }
            }
        });

        Map.Entry<String, Integer> minValue = nodeWeights.entrySet().stream()
                .min(Comparator.comparing(Map.Entry::getValue)).orElse(null);

        if( minValue == null || minValue.getValue().equals( nodeWeights.get(ME))) {
            return ME;
        }
        return minValue.getKey();
    }

    private boolean contains(
            List<QueryRegistrationCache.QueryRegistration> queries, String componentName) {
        return queries.size() > 0 && queries.get(0).getQueryHandler().getComponentName().equals(componentName);
    }

    @Override
    public boolean canRebalance(String clientName, String componentName, List<String> activeNodes) {
        logger.debug("Trying to rebalance {}/{}, active AxonHub nodes: {}", clientName, componentName, activeNodes);
        String assignedNode = selectNode(clientName, componentName, activeNodes);
        logger.debug("Result for rebalance {}/{}, {}", clientName, componentName, assignedNode);

        return ! assignedNode.equals(ME);
    }
}
