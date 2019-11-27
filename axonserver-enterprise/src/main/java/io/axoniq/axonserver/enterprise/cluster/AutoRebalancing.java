package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.licensing.Feature;
import io.axoniq.axonserver.config.FeatureChecker;
import io.axoniq.axonserver.grpc.PlatformService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Set;

/**
 * Component that balances client application connections across Axon Server nodes.
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
        Set<PlatformService.ClientComponent> connectedClients = platformService.getConnectedClients();
        logger.debug("Rebalance: {}", connectedClients);
        connectedClients.stream()
                        .filter(e -> nodeSelector.canRebalance(e.getClient(), e.getComponent(), e.getContext()))
                        .findFirst()
                        .ifPresent(platformService::requestReconnect);
    }
}
