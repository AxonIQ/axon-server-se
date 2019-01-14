package io.axoniq.sample;

import org.axonframework.axonserver.connector.AxonServerConfiguration;
import org.axonframework.messaging.MetaData;
import org.axonframework.queryhandling.QueryHandler;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;


/**
 * @author Marc Gathier
 */
@Component
@Profile("queries")
public class Queries {

    private final AxonServerConfiguration messagingConfiguration;

    public Queries(AxonServerConfiguration messagingConfiguration) {
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
