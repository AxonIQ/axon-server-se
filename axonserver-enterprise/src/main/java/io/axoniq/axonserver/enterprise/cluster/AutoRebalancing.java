package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.features.Feature;
import io.axoniq.axonserver.features.FeatureChecker;
import io.axoniq.axonserver.grpc.PlatformService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Set;

/**
 * Author: marc
 */
@Component
public class AutoRebalancing  {
    private final Logger logger = LoggerFactory.getLogger(AutoRebalancing.class);

    private final PlatformService platformService;
    private final ClusterController clusterController;
    private final FeatureChecker featureChecker;

    private final boolean enabled;

    public AutoRebalancing(PlatformService platformService,
                           ClusterController clusterController,
                           FeatureChecker featureChecker,
                           @Value("${axoniq.axonserver.cluster.auto-balancing:true}") boolean enabled) {
        this.platformService = platformService;
        this.clusterController = clusterController;
        this.featureChecker = featureChecker;
        this.enabled = enabled;
    }


    @Scheduled(fixedRateString = "${axoniq.axonserver.cluster.balancing-rate:15000}")
    protected void rebalance() {
        if( !Feature.CONNECTION_BALANCING.enabled(featureChecker) || !enabled) return;
        Set<PlatformService.ClientComponent> connectedClients = platformService.getConnectedClients();
        logger.debug("Rebalance: {}", connectedClients);
        connectedClients.stream().filter(e -> clusterController.canRebalance(e.getClient(), e.getComponent(), e.getContext())).findFirst()
                .ifPresent(platformService::requestReconnect);
    }
}
