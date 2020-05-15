package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.config.FeatureChecker;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.grpc.PlatformService;
import io.axoniq.axonserver.licensing.Feature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Component that balances client application connections across Axon Server nodes.
 * <p>
 * Since 4.3 the rebalancing is only possible for applications that were connected to this axon server node at the
 * moment
 * another Axon Server node is connected. Once an application is selected for rebalancing it will no longer be eligible
 * for rebalancing until another Axon Server node is started.
 *
 * @author Marc Gathier
 * @since 4.0
 */
@Component
public class AutoRebalancing {

    private final Logger logger = LoggerFactory.getLogger(AutoRebalancing.class);

    private final PlatformService platformService;
    private final NodeSelector nodeSelector;
    private final FeatureChecker featureChecker;

    /**
     * Set of clients that may be rebalanced.
     */
    private final Set<PlatformService.ClientComponent> clientWhitelist = new CopyOnWriteArraySet<>();

    private final boolean enabled;

    /**
     * Constructor
     *
     * @param platformService to send message to client
     * @param nodeSelector    checks if a client should reconnect
     * @param featureChecker  checks if the feature is available based on the Axon Server version
     * @param enabled         determines if auto re-balancing should be done
     */
    public AutoRebalancing(PlatformService platformService,
                           NodeSelector nodeSelector,
                           FeatureChecker featureChecker,
                           @Value("${axoniq.axonserver.cluster.auto-balancing:true}") boolean enabled) {
        this.platformService = platformService;
        this.nodeSelector = nodeSelector;
        this.featureChecker = featureChecker;
        this.enabled = enabled;
    }


    /**
     * Finds the first client that is eligible for moving to another Axon Server node and, if found, sends a request
     * to reconnect to that client.
     * Running at a fixed rate when the property "axoniq.axonserver.cluster.auto-balancing" is true.
     */
    @Scheduled(fixedRateString = "${axoniq.axonserver.cluster.balancing-rate:15000}")
    protected void rebalance() {
        if (!Feature.CONNECTION_BALANCING.enabled(featureChecker) || !enabled) {
            return;
        }
        logger.debug("Rebalance: {}", clientWhitelist);
        clientWhitelist.stream()
                       .filter(e -> nodeSelector.canRebalance(e.getClient(), e.getComponent(), e.getContext()))
                       .findFirst()
                       .ifPresent(this::requestReconnect);
    }

    private void requestReconnect(PlatformService.ClientComponent clientComponent) {
        if (clientWhitelist.remove(clientComponent)) {
            logger.info("Requesting reconnect for {}", clientComponent.getClient());
            platformService.requestReconnect(clientComponent);
        }
    }

    /**
     * Resets the whitelist for clients eligible for rebalance to the currently connected clients.
     *
     * @param axonServerNodeConnected the connected axon server node
     */
    @EventListener
    public void on(ClusterEvents.AxonServerNodeConnected axonServerNodeConnected) {
        clientWhitelist.clear();
        clientWhitelist.addAll(platformService.getConnectedClients());
    }

    @EventListener
    public void on(TopologyEvents.ApplicationDisconnected applicationDisconnected) {
        clientWhitelist.remove(new PlatformService.ClientComponent(applicationDisconnected.getClient(),
                                                                   applicationDisconnected.getComponentName(),
                                                                   applicationDisconnected.getContext()));
    }
}
