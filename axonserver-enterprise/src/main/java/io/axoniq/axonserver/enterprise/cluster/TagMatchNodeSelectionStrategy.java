package io.axoniq.axonserver.enterprise.cluster;

//import io.axoniq.axonserver.ClusterTagsCache;

import io.axoniq.axonserver.enterprise.component.connection.ConnectionProvider;
import io.axoniq.axonserver.enterprise.component.connection.rule.Rule;
import io.axoniq.axonserver.enterprise.component.connection.rule.RuleBasedConnectionProvider;
import io.axoniq.axonserver.message.ClientIdentification;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

import java.util.Collection;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * @author Sara Pellegrini
 * @since 4.2
 */
@ConditionalOnProperty(value = "axoniq.axonserver.clients-connection-strategy", havingValue = "tagsMatch")
//@Component
public class TagMatchNodeSelectionStrategy implements NodeSelectionStrategy {

    @Nonnull private final ConnectionProvider connectionProvider;

    @Nonnull private final String thisNodeName;

//    @Autowired
//    public TagMatchNodeSelectionStrategy(ClusterTagsCache clusterTags, ClientTagsCache clientsTags,
//                                         MessagingPlatformConfiguration configuration) {
//        this(new TagsMatch(node -> clusterTags.getClusterTags().get(node), clientsTags), configuration.getName());
//    }

    public TagMatchNodeSelectionStrategy(Rule tagsMatchRule, String thisNodeName) {
        this(new RuleBasedConnectionProvider(tagsMatchRule), thisNodeName);
    }

    public TagMatchNodeSelectionStrategy(@Nonnull ConnectionProvider connectionProvider, @Nonnull String thisNode) {
        this.connectionProvider = connectionProvider;
        this.thisNodeName = thisNode;
    }

    @Override
    public String selectNode(ClientIdentification client, String component, Collection<String> nodes) {
        return connectionProvider.bestMatch(client, nodes);
    }

    @Override
    public boolean canRebalance(ClientIdentification client, String component, List<String> nodes) {
        return !thisNodeName.equals(selectNode(client, component, nodes));
    }
}
