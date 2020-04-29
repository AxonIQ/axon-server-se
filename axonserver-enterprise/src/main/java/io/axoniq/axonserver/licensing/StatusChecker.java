package io.axoniq.axonserver.licensing;

import io.axoniq.axonserver.LifecycleController;
import io.axoniq.axonserver.config.FeatureChecker;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;

import java.util.stream.Stream;

/**
 * @author Marc Gathier
 * @author Stefan Dragisic
 */
@Controller
public class StatusChecker {
    private final Logger log = LoggerFactory.getLogger(StatusChecker.class);
    private final FeatureChecker limits;
    private final ClusterController clusterController;

    public StatusChecker(FeatureChecker limits, ClusterController clusterController) {
        this.limits = limits;
        this.clusterController = clusterController;
    }

    // not configurable, as must not be changed by customer
    @Scheduled(fixedRate = 3600000, initialDelay = 3600000)
    protected void checkLicense() {
            int allowedClusterNodes = limits.getMaxClusterSize();
            long clusterNodes = clusterController.nodes().count();

            if (clusterController.isAdminNode() && (clusterNodes > allowedClusterNodes) ) {
                log.warn("Cluster limited to " + allowedClusterNodes + " but found " + clusterNodes + " nodes.");
                Stream<ClusterNode> nodesToDelete = clusterController.nodes().filter(it -> !it.equals(clusterController.getMe())).limit(clusterNodes - allowedClusterNodes);

                nodesToDelete.forEach(it-> {
                    log.warn("Removing " + it.getName() + " node from cluster");
                    clusterController.deleteNode(it.getName());
                });
            }
    }

}
