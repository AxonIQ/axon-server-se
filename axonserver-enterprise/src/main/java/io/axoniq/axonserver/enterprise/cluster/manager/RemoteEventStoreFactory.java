package io.axoniq.axonserver.enterprise.cluster.manager;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.messaging.event.RemoteEventStore;
import io.axoniq.axonserver.enterprise.replication.ContextLeaderProvider;
import io.axoniq.axonserver.grpc.ChannelProvider;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.message.event.EventStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Factory to create a remote event store object to communicate to an event store on another Axon Server node.
 *
 * @author Marc Gathier
 * @since 4.4
 */
@Component
public class RemoteEventStoreFactory {

    private final String internalToken;
    private final Function<String, ClusterNode> clusterNodeSupplier;
    private final ChannelProvider channelProvider;
    private final LocalEventStore localEventStore;
    private final BiFunction<String, Boolean, String> leaderProvider;

    /**
     * Custom constructor for testing purposes
     *
     * @param internalToken       the internal token used for communication between Axon Server nodes
     * @param clusterNodeSupplier provides cluster node data based on the node name
     * @param channelProvider     provides a channel to a remote node
     */
    public RemoteEventStoreFactory(String internalToken,
                                   Function<String, ClusterNode> clusterNodeSupplier,
                                   ChannelProvider channelProvider,
                                   LocalEventStore localEventStore,
                                   BiFunction<String, Boolean, String> leaderProvider) {
        this.internalToken = internalToken;
        this.clusterNodeSupplier = clusterNodeSupplier;
        this.channelProvider = channelProvider;
        this.localEventStore = localEventStore;
        this.leaderProvider = leaderProvider;
    }

    /**
     * @param messagingPlatformConfiguration provides the internal token
     * @param clusterController              provides node information
     * @param channelProvider                provides channel to a remote node
     */
    @Autowired
    public RemoteEventStoreFactory(MessagingPlatformConfiguration messagingPlatformConfiguration,
                                   ClusterController clusterController,
                                   ChannelProvider channelProvider,
                                   LocalEventStore localEventStore,
                                   ContextLeaderProvider leaderProvider) {
        this(messagingPlatformConfiguration.getAccesscontrol().getInternalToken(),
             clusterController::getNode,
             channelProvider, localEventStore, leaderProvider::getLeaderOrWait);
    }

    /**
     * Creates an Event Store facade for an event store on a remote node.
     *
     * @param node the name of the node
     * @return an Event Store facade for an event store on a remote node
     */
    public EventStore create(String node) {
        return new RemoteEventStore(clusterNodeSupplier.apply(node), internalToken, channelProvider,
                                    localEventStore, leaderProvider, clusterNodeSupplier);
    }
}
