package io.axoniq.axonserver.enterprise.cluster.raftfacade;

import io.axoniq.axonserver.component.processor.balancing.jpa.LoadBalancingStrategy;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.LoadBalanceStrategyController;
import io.axoniq.axonserver.rest.LoadBalanceStrategyControllerFacade;
import io.axoniq.axonserver.serializer.Printable;

/**
 * @author Marc Gathier
 * @since 4.1
 */
public class RaftLoadBalanceStrategyControllerFacade implements LoadBalanceStrategyControllerFacade {

    private final LoadBalanceStrategyController controller;

    public RaftLoadBalanceStrategyControllerFacade(LoadBalanceStrategyController controller) {
        this.controller = controller;
    }

    @Override
    public Iterable<? extends Printable> findAll() {
        return controller.findAll();
    }


    @Override
    public LoadBalancingStrategy findByName(String strategyName) {
        return controller.findByName(strategyName);
    }
}
