package io.axoniq.axonserver.topology;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;

/**
 * @author Marc Gathier
 */
public class DefaultTopology implements Topology {
    private final AxonServerNode me;

    public DefaultTopology(MessagingPlatformConfiguration configuration) {
        this(new SimpleAxonServerNode(configuration.getName(), configuration.getFullyQualifiedHostname(), configuration.getPort(),configuration.getHttpPort()));
    }

    public DefaultTopology(AxonServerNode me) {
        this.me = me;
    }

    @Override
    public String getName() {
        return me.getName();
    }

    @Override
    public AxonServerNode getMe() {
        return me;
    }
}
