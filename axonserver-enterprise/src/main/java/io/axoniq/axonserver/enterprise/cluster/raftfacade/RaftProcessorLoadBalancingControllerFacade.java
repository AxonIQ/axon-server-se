package io.axoniq.axonserver.enterprise.cluster.raftfacade;

import io.axoniq.axonserver.enterprise.cluster.RaftConfigServiceFactory;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.ProcessorLoadBalancing;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.ProcessorLoadBalancingController;
import io.axoniq.axonserver.grpc.ProcessorLBStrategyConverter;
import io.axoniq.axonserver.rest.ProcessorLoadBalancingControllerFacade;

import java.util.List;

/**
 * Author: marc
 */
public class RaftProcessorLoadBalancingControllerFacade implements ProcessorLoadBalancingControllerFacade {

    private final ProcessorLoadBalancingController processorLoadBalancingController;
    private final RaftConfigServiceFactory raftServiceFactory;

    public RaftProcessorLoadBalancingControllerFacade(ProcessorLoadBalancingController processorLoadBalancingController,
                                                      RaftConfigServiceFactory raftServiceFactory) {
        this.processorLoadBalancingController = processorLoadBalancingController;
        this.raftServiceFactory = raftServiceFactory;
    }

    @Override
    public void save(ProcessorLoadBalancing processorLoadBalancing) {
        raftServiceFactory.getRaftConfigService().updateProcessorLoadBalancing(ProcessorLBStrategyConverter
                                                                                       .createProcessorLBStrategy(processorLoadBalancing));

    }

    @Override
    public List<ProcessorLoadBalancing> findByComponentAndContext(String component, String context) {
        return processorLoadBalancingController.findByComponentAndContext(component, context);
    }
}
