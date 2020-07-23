package io.axoniq.axonserver.enterprise.cluster.raftfacade;

import io.axoniq.axonserver.enterprise.replication.admin.RaftConfigServiceFactory;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.AdminProcessorLoadBalancing;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.AdminProcessorLoadBalancingService;
import io.axoniq.axonserver.grpc.ProcessorLBStrategyConverter;
import io.axoniq.axonserver.rest.ProcessorLoadBalancingControllerFacade;

import java.util.List;

/**
 * @author Marc Gathier
 */
public class RaftProcessorLoadBalancingControllerFacade implements ProcessorLoadBalancingControllerFacade {

    private final AdminProcessorLoadBalancingService processorLoadBalancingService;
    private final RaftConfigServiceFactory raftServiceFactory;

    public RaftProcessorLoadBalancingControllerFacade(AdminProcessorLoadBalancingService processorLoadBalancingService,
                                                      RaftConfigServiceFactory raftServiceFactory) {
        this.processorLoadBalancingService = processorLoadBalancingService;
        this.raftServiceFactory = raftServiceFactory;
    }

    @Override
    public void save(AdminProcessorLoadBalancing processorLoadBalancing) {
        raftServiceFactory.getRaftConfigService().updateProcessorLoadBalancing(ProcessorLBStrategyConverter
                                                                                       .createProcessorLBStrategy(
                                                                                               processorLoadBalancing));
    }

    @Override
    public List<AdminProcessorLoadBalancing> findByContext(String context) {
        return processorLoadBalancingService.findByContext(context);
    }
}
