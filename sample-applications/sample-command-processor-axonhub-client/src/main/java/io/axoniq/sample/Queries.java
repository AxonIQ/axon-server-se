package io.axoniq.sample;

import org.axonframework.axonserver.connector.AxonServerConfiguration;
import org.axonframework.messaging.MetaData;
import org.axonframework.queryhandling.QueryHandler;
import org.springframework.stereotype.Component;


/**
 * Author: marc
 */
@Component
public class Queries {

    private final AxonServerConfiguration messagingConfiguration;

    public Queries(AxonServerConfiguration messagingConfiguration) {
        this.messagingConfiguration = messagingConfiguration;
    }

    @QueryHandler
    public String echo(String cmd, MetaData metaData) {
        if( cmd.equals("test")) throw new IllegalStateException("Cannot process test");
        return cmd + "@" + messagingConfiguration.getComponentName();
    }

    @QueryHandler
    public int echo2( String cmd, MetaData metaData) {
        if( cmd.equals("test")) throw new IllegalStateException("Cannot convert test to int");
        return metaData.size() + cmd.length();
    }

    @QueryHandler
    public Double queryForNumber(FindNumberQuery query, MetaData metaData) {
        System.out.println(metaData.get("nodeId"));
        return Double.valueOf(10);
    }
}
