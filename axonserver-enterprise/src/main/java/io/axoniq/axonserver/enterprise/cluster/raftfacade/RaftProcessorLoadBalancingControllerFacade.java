package io.axoniq.axonserver.enterprise.cluster.raftfacade;

import io.axoniq.axonserver.enterprise.cluster.RaftConfigServiceFactory;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.ProcessorLoadBalancing;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.ProcessorLoadBalancingService;
import io.axoniq.axonserver.grpc.ProcessorLBStrategyConverter;
import io.axoniq.axonserver.rest.ProcessorLoadBalancingControllerFacade;

import java.util.List;

/**
 * @author Marc Gathier
 */
public class RaftProcessorLoadBalancingControllerFacade implements ProcessorLoadBalancingControllerFacade {

    private final ProcessorLoadBalancingService processorLoadBalancingService;
    private final RaftConfigServiceFactory raftServiceFactory;

    public RaftProcessorLoadBalancingControllerFacade(ProcessorLoadBalancingService processorLoadBalancingService,
                                                      RaftConfigServiceFactory raftServiceFactory) {
        this.processorLoadBalancingService = processorLoadBalancingService;
        this.raftServiceFactory = raftServiceFactory;
    }

    @Override
    public void save(ProcessorLoadBalancing processorLoadBalancing) {
        raftServiceFactory.getRaftConfigService().updateProcessorLoadBalancing(ProcessorLBStrategyConverter
                                                                                       .createProcessorLBStrategy(processorLoadBalancing));

    }

    @Override
    public List<ProcessorLoadBalancing> findByContext(String context) {
        return processorLoadBalancingService.findByContext(context);
    }
}
