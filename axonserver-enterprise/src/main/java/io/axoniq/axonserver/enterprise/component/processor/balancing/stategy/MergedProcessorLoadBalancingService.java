package io.axoniq.axonserver.enterprise.component.processor.balancing.stategy;

import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.AdminProcessorLoadBalancing;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.BaseProcessorLoadBalancing;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.ReplicationGroupProcessorLoadBalancing;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;

/**
 * Service to retrieve {@link io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.BaseProcessorLoadBalancing}
 * definitions
 * for a context. If the node is an admin node it will check the {@link AdminProcessorLoadBalancing}
 * objects, otherwise it will use the {@link ReplicationGroupProcessorLoadBalancing}
 * items.
 *
 * @author Marc Gathier
 * @since 4.2
 */
@Service
public class MergedProcessorLoadBalancingService {

    private final AdminProcessorLoadBalancingService processorLoadBalancingService;
    private final ReplicationGroupProcessorLoadBalancingService raftProcessorLoadBalancingService;

    private final ClusterController clusterController;

    /**
     * Constructor for the service
     *
     * @param processorLoadBalancingService     service to access the admin table
     * @param raftProcessorLoadBalancingService service to access the raft specific table
     * @param clusterController                 controller to determine if the current node is an admin node
     */
    public MergedProcessorLoadBalancingService(
            AdminProcessorLoadBalancingService processorLoadBalancingService,
            ReplicationGroupProcessorLoadBalancingService raftProcessorLoadBalancingService,
            ClusterController clusterController) {
        this.processorLoadBalancingService = processorLoadBalancingService;
        this.raftProcessorLoadBalancingService = raftProcessorLoadBalancingService;
        this.clusterController = clusterController;
    }

    /**
     * Gets the load balancing definitions per processor for the processors configured in this context.
     *
     * @param context the name of the context
     * @return list of processors and their auto-loadbalance strategy
     */
    public List<? extends BaseProcessorLoadBalancing> findByContext(String context) {
        if (clusterController.isAdminNode()) {
            return processorLoadBalancingService.findByContext(context);
        }

        return raftProcessorLoadBalancingService.findByContext(Collections.singletonList(context));
    }
}
