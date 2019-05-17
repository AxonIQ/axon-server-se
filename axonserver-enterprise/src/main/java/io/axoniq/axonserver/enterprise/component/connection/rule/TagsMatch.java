package io.axoniq.axonserver.enterprise.component.connection.rule;

import io.axoniq.axonserver.message.ClientIdentification;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * @author Sara Pellegrini
 * @since 4.2
 */
public class TagsMatch implements Rule {

    private final Function<String, Map<String, String>> clusterTagsProvider;
    private final Function<ClientIdentification, Map<String, String>> clientTagsProvider;

    public TagsMatch(Function<String, Map<String, String>> clusterTagsProvider,
                     Function<ClientIdentification, Map<String, String>> clientTagsProvider) {
        this.clusterTagsProvider = clusterTagsProvider;
        this.clientTagsProvider = clientTagsProvider;
    }

    @Override
    public ConnectionValue apply(ClientIdentification client, String server) {
        return () -> {
            double connectionValue = 0;
            Map<String, String> clientTags = clientTagsProvider.apply(client);
            for (String tag : clientTags.keySet()) {
                String clientTag = clientTags.get(tag);
                String nodeTag = clusterTagsProvider.apply(server).get(tag);
                double tagValue = Objects.equals(clientTag, nodeTag) ? 1 : 0;
                connectionValue += tagValue;
            }
            return connectionValue;
        };
    }
}
