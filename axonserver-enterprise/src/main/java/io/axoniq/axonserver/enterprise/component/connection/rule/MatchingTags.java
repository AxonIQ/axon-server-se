package io.axoniq.axonserver.enterprise.component.connection.rule;

import io.axoniq.axonserver.message.ClientIdentification;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * {@link Rule} implementation that calculates the {@link ConnectionValue}
 * as the number of matching tags between client and server.
 *
 * @author Sara Pellegrini
 * @since 4.2
 */
public class MatchingTags implements Rule {

    private final Function<String, Map<String, String>> clusterTagsProvider;
    private final Function<ClientIdentification, Map<String, String>> clientTagsProvider;

    /**
     * Creates an instance based on the cluster and clients tags providers.
     *
     * @param clusterTagsProvider the provider for cluster tags
     * @param clientTagsProvider  the provider for clients tags
     */
    public MatchingTags(Function<String, Map<String, String>> clusterTagsProvider,
                        Function<ClientIdentification, Map<String, String>> clientTagsProvider) {
        this.clusterTagsProvider = clusterTagsProvider;
        this.clientTagsProvider = clientTagsProvider;
    }

    /**
     * Returns the number of the matching tags between client and server
     *
     * @param client the client
     * @param server the axon server node instance
     * @return the number of the matching tags between client and server
     */
    @Override
    public ConnectionValue apply(ClientIdentification client, String server) {
        return () -> {
            double connectionValue = 0;
            Map<String, String> clientTags = clientTagsProvider.apply(client);
            Map<String, String> nodeTags = clusterTagsProvider.apply(server);

            if (clientTags == null || nodeTags == null) {
                return 0;
            }
            for (String tag : clientTags.keySet()) {
                String clientTag = clientTags.get(tag);
                String nodeTag = nodeTags.get(tag);
                double tagValue = Objects.equals(clientTag, nodeTag) ? 1 : 0;
                connectionValue += tagValue;
            }
            return connectionValue;
        };
    }
}
