package io.axoniq.axonserver.enterprise.cluster.raftfacade;

import io.axoniq.axonserver.component.processor.balancing.jpa.LoadBalancingStrategy;
import io.axoniq.axonserver.enterprise.cluster.RaftConfigServiceFactory;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.LoadBalanceStrategyController;
import io.axoniq.axonserver.grpc.LoadBalancingStrategyConverter;
import io.axoniq.axonserver.grpc.internal.LoadBalanceStrategy;
import io.axoniq.axonserver.rest.LoadBalanceStrategyControllerFacade;
import io.axoniq.axonserver.serializer.Printable;

/**
 * Author: marc
 */
public class RaftLoadBalanceStrategyControllerFacade implements LoadBalanceStrategyControllerFacade {

    private final LoadBalanceStrategyController controller;
    private final RaftConfigServiceFactory raftServiceFactory;

    public RaftLoadBalanceStrategyControllerFacade(LoadBalanceStrategyController controller,
                                                   RaftConfigServiceFactory raftServiceFactory) {
        this.controller = controller;
        this.raftServiceFactory = raftServiceFactory;
    }

    @Override
    public Iterable<? extends Printable> findAll() {
        return controller.findAll();
    }

    @Override
    public void save(LoadBalancingStrategy loadBalancingStrategy) {
        raftServiceFactory.getRaftConfigService().updateLoadBalancingStrategy(LoadBalancingStrategyConverter
                                                                                      .createLoadBalanceStrategy(loadBalancingStrategy));
    }

    @Override
    public void delete(String strategyName) {
        raftServiceFactory.getRaftConfigService().deleteLoadBalancingStrategy(LoadBalanceStrategy.newBuilder().setName(strategyName).build());

    }

    @Override
    public void updateFactoryBean(String strategyName, String factoryBean) {
        LoadBalanceStrategy current = LoadBalancingStrategyConverter.createLoadBalanceStrategy(controller.findByName(strategyName));
        raftServiceFactory.getRaftConfigService().updateLoadBalancingStrategy(LoadBalanceStrategy.newBuilder(current).setFactoryBean(factoryBean).build());

    }

    @Override
    public void updateLabel(String strategyName, String label) {
        LoadBalanceStrategy current = LoadBalancingStrategyConverter.createLoadBalanceStrategy(controller.findByName(strategyName));
        raftServiceFactory.getRaftConfigService().updateLoadBalancingStrategy(LoadBalanceStrategy.newBuilder(current).setLabel(label).build());
    }

    @Override
    public LoadBalancingStrategy findByName(String strategyName) {
        return controller.findByName(strategyName);
    }
}
