package io.axoniq.axonserver.enterprise.storage.multitier;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.messaging.event.RemoteLowerTierEventStore;
import io.axoniq.axonserver.grpc.ChannelProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.function.Function;

/**
 * Factory to create a remote event store object to communicate to an event store on another Axon Server node.
 *
 * @author Marc Gathier
 * @since 4.4
 */
@Component
public class LowerTierEventStoreFactory {

    private final String internalToken;
    private final Function<String, ClusterNode> clusterNodeSupplier;
    protected final ChannelProvider channelProvider;

    /**
     * Custom constructor for testing purposes
     *
     * @param internalToken       the internal token used for communication between Axon Server nodes
     * @param clusterNodeSupplier provides cluster node data based on the node name
     * @param channelProvider     provides a channel to a remote node
     */
    public LowerTierEventStoreFactory(String internalToken,
                                      Function<String, ClusterNode> clusterNodeSupplier,
                                      ChannelProvider channelProvider) {
        this.internalToken = internalToken;
        this.clusterNodeSupplier = clusterNodeSupplier;
        this.channelProvider = channelProvider;
    }

    /**
     * @param messagingPlatformConfiguration provides the internal token
     * @param clusterController              provides node information
     * @param channelProvider                provides channel to a remote node
     */
    @Autowired
    public LowerTierEventStoreFactory(MessagingPlatformConfiguration messagingPlatformConfiguration,
                                      ClusterController clusterController,
                                      ChannelProvider channelProvider) {
        this(messagingPlatformConfiguration.getAccesscontrol().getInternalToken(),
             clusterController::getNode,
             channelProvider);
    }

    /**
     * Creates an Event Store facade for an event store on a remote node.
     *
     * @param node the name of the node
     * @return an Event Store facade for an event store on a remote node
     */
    public RemoteLowerTierEventStore create(String node) {
        return new RemoteLowerTierEventStore(clusterNodeSupplier.apply(node), internalToken, channelProvider);
    }
}
