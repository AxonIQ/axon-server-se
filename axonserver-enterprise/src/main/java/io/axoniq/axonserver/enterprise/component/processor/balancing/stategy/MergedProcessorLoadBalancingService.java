package io.axoniq.axonserver.enterprise.component.processor.balancing.stategy;

import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.BaseProcessorLoadBalancing;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Service to retrieve {@link io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.BaseProcessorLoadBalancing}
 * definitions
 * for a context. If the node is an admin node it will check the {@link io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.ProcessorLoadBalancing}
 * objects, otherwise it will use the {@link io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.RaftProcessorLoadBalancing}
 * items.
 *
 * @author Marc Gathier
 * @since 4.2
 */
@Service
public class MergedProcessorLoadBalancingService {

    private final ProcessorLoadBalancingService processorLoadBalancingService;
    private final RaftProcessorLoadBalancingService raftProcessorLoadBalancingService;

    private final ClusterController clusterController;

    /**
     * Constructor for the service
     *
     * @param processorLoadBalancingService     service to access the admin table
     * @param raftProcessorLoadBalancingService service to access the raft specific table
     * @param clusterController                 controller to determine if the current node is an admin node
     */
    public MergedProcessorLoadBalancingService(
            ProcessorLoadBalancingService processorLoadBalancingService,
            RaftProcessorLoadBalancingService raftProcessorLoadBalancingService,
            ClusterController clusterController) {
        this.processorLoadBalancingService = processorLoadBalancingService;
        this.raftProcessorLoadBalancingService = raftProcessorLoadBalancingService;
        this.clusterController = clusterController;
    }

    public List<? extends BaseProcessorLoadBalancing> findByContext(String context) {
        if (clusterController.isAdminNode()) {
            return processorLoadBalancingService.findByContext(context);
        }

        return raftProcessorLoadBalancingService.findByContext(context);
    }
}
