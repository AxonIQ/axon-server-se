package io.axoniq.sample;

import org.axonframework.axonserver.connector.AxonServerConfiguration;
import org.axonframework.messaging.MetaData;
import org.axonframework.queryhandling.QueryHandler;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.stream.IntStream;


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
        StringBuilder result = new StringBuilder();
        IntStream.range(0, cmd.length() * 100).forEach(i -> result.append(i).append(cmd));

        return result.toString();
    }

    @QueryHandler
    public int echo2( String cmd, MetaData metaData) {
        return metaData.size() + cmd.length();
    }
}
