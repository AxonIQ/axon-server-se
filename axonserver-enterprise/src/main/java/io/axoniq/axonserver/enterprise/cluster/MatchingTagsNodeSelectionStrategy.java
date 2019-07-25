package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.ClusterTagsCache;
import io.axoniq.axonserver.component.tags.ClientTagsCache;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.component.connection.ConnectionProvider;
import io.axoniq.axonserver.enterprise.component.connection.rule.MatchingTags;
import io.axoniq.axonserver.enterprise.component.connection.rule.Rule;
import io.axoniq.axonserver.enterprise.component.connection.rule.RuleBasedConnectionProvider;
import io.axoniq.axonserver.message.ClientIdentification;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * Implementation of {@link NodeSelectionStrategy} that chooses the node with the highest number of tags matching.
 *
 * @author Sara Pellegrini
 * @since 4.2
 */
@Primary
@ConditionalOnProperty(value = "axoniq.axonserver.clients-connection-strategy", havingValue = "matchingTags")
@Component
public class MatchingTagsNodeSelectionStrategy implements NodeSelectionStrategy {

    @Nonnull private final NodeSelectionStrategy nodeSelectionStrategy;

    @Nonnull private final ConnectionProvider connectionProvider;

    @Nonnull private final String thisNodeName;

    /**
     * Default contructor that uses the {@link MatchingTags} rule to find the best node.
     *
     * @param clusterTags   the provider of cluster tags
     * @param clientsTags   the provider of clients tags
     * @param configuration the messaging platform configuration
     * @param nodeSelectionStrategy the fallback strategy to use after the MatchingTags
     *                                                    check
     */
    @Autowired
    public MatchingTagsNodeSelectionStrategy(ClusterTagsCache clusterTags, ClientTagsCache clientsTags,
                                             MessagingPlatformConfiguration configuration,
                                             NodeSelectionStrategy nodeSelectionStrategy) {
        this(new MatchingTags(node -> clusterTags.getClusterTags().get(node), clientsTags), configuration.getName(),
             nodeSelectionStrategy);
    }

    /**
     * Creates an instance based on a {@link RuleBasedConnectionProvider}
     * that use the specified rule to calculate the value of each node.
     *
     * @param tagsMatchRule a {@link Rule} that returns a value of each node equals to the number of tags matching
     * @param thisNodeName the local node identifier
     * @param nodeSelectionStrategy the fallback strategy to use after the MatchingTags
     *                                                    check
     */
    public MatchingTagsNodeSelectionStrategy(Rule tagsMatchRule, String thisNodeName,
                                             NodeSelectionStrategy nodeSelectionStrategy) {
        this(new RuleBasedConnectionProvider(tagsMatchRule), thisNodeName, nodeSelectionStrategy);
    }

    /**
     * Base constructor that creates an instance with the specified {@code connectionProvider} and node name.
     *
     * @param connectionProvider the {@link ConnectionProvider} that return the node with the highest number of tags matching
     * @param thisNode the local node identifier
     * @param nodeSelectionStrategy the fallback strategy to use after the MatchingTags
     *                                                    check
     */
    public MatchingTagsNodeSelectionStrategy(@Nonnull ConnectionProvider connectionProvider, @Nonnull String thisNode,
                                             @Nonnull NodeSelectionStrategy nodeSelectionStrategy) {
        this.connectionProvider = connectionProvider;
        this.thisNodeName = thisNode;
        this.nodeSelectionStrategy = nodeSelectionStrategy;
    }

    /**
     * Returns the identifier of the node with the highest number of tags matching with the specified client. If
     * multiple nodes have the same number of matching tags then the choice of node is delegated to
     * {@link SubscriptionCountBasedNodeSelectionStrategy}.
     *
     * @param client the client identifier
     * @param component the client's component name
     * @param nodes the cluster nodes currently active
     * @return the identifier of the node with the highest number of tags matching
     */
    @Override
    public String selectNode(ClientIdentification client, String component, Collection<String> nodes) {
        List<String> matchingNodes = connectionProvider.bestMatches(client, nodes);

        return nodeSelectionStrategy.selectNode(client, component, matchingNodes);
    }

    /**
     * Returns if it is needed to move the specified client to another node of the cluster.
     *
     * @param client the client identifier
     * @param component the client's component name
     * @param nodes the cluster nodes currently active
     * @return true if there is another active node that fits better then the local one, false otherwise
     */
    @Override
    public boolean canRebalance(ClientIdentification client, String component, List<String> nodes) {
        return !thisNodeName.equals(selectNode(client, component, nodes));
    }
}
