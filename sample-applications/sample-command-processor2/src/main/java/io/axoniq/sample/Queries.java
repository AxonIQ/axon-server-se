package io.axoniq.sample;

import io.axoniq.axonhub.client.AxonHubConfiguration;
import org.axonframework.messaging.MetaData;
import org.axonframework.queryhandling.QueryHandler;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;


/**
 * Author: marc
 */
@Component
@Profile("queries")
public class Queries {

    private final AxonHubConfiguration messagingConfiguration;

    public Queries(AxonHubConfiguration messagingConfiguration) {
        this.messagingConfiguration = messagingConfiguration;
    }

    @QueryHandler
    public String echo(String cmd, MetaData metaData) {
        return cmd + "@" + messagingConfiguration.getComponentName();
    }

    @QueryHandler
    public int echo2( String cmd, MetaData metaData) {
        return metaData.size() + cmd.length();
    }
}
