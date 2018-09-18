package io.axoniq.axonserver.component.processor.balancing.jpa;

import io.axoniq.axonserver.LoadBalancingSynchronizationEvents;
import io.axoniq.axonserver.grpc.LoadBalancingStrategyProtoConverter;
import io.axoniq.platform.application.ApplicationModelController;
import io.axoniq.axonserver.grpc.internal.Action;
import io.axoniq.axonserver.grpc.internal.LoadBalanceStrategy;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Created by Sara Pellegrini on 16/08/2018.
 * sara.pellegrini@gmail.com
 */
@Component
public class LoadBalanceStrategyController {

    private final ProcessorLoadBalancingController processorLoadBalancingController;

    private final ApplicationModelController applicationController;

    private final LoadBalanceStrategyRepository repository;

    private final ApplicationEventPublisher eventPublisher;

    public LoadBalanceStrategyController(
            ProcessorLoadBalancingController processorLoadBalancingController,
            ApplicationModelController applicationController,
            LoadBalanceStrategyRepository repository,
            ApplicationEventPublisher eventPublisher) {
        this.processorLoadBalancingController = processorLoadBalancingController;
        this.applicationController = applicationController;
        this.repository = repository;
        this.eventPublisher = eventPublisher;
    }

    public void save(LoadBalancingStrategy strategy){
        repository.save(strategy);
        sync(strategy.name(), Action.MERGE);
    }

    private void sync(String strategyName, Action action) {
        applicationController.incrementModelVersion(LoadBalancingStrategy.class);
        LoadBalanceStrategy loadBalanceStrategy = null;
        switch (action){
            case MERGE:
                LoadBalancingStrategy strategy = repository.findByName(strategyName);
                loadBalanceStrategy = new LoadBalancingStrategyProtoConverter().unmap(strategy);
                loadBalanceStrategy = LoadBalanceStrategy.newBuilder(loadBalanceStrategy).setAction(action).build();
                break;
            case DELETE:
                loadBalanceStrategy = LoadBalanceStrategy.newBuilder().setName(strategyName)
                                                         .setAction(Action.DELETE).build();
                break;
        }
        eventPublisher.publishEvent(new LoadBalancingSynchronizationEvents.LoadBalancingStrategyReceived(loadBalanceStrategy, false));
    }

    public Iterable<LoadBalancingStrategy> findAll() {
        return repository.findAll(new Sort("id"));
    }

    public void delete(String strategyName) {
        if ("default".equals(strategyName)){
            throw new IllegalArgumentException("Cannot delete default load balancing strategy.");
        }

        List<ProcessorLoadBalancing> processors = processorLoadBalancingController.findByStrategy(strategyName);
        if (processors.size() > 0){
            throw new IllegalStateException("Impossible to delete " + strategyName + " because is currently used.");
        }

        repository.deleteByName(strategyName);
        sync(strategyName, Action.DELETE);
    }

    public void updateFactoryBean(String strategyName, String factoryBean) {
        repository.updateFactoryBean(strategyName, factoryBean);
        sync(strategyName, Action.MERGE);
    }

    public void updateLabel(String strategyName, String label) {
        repository.updateLabel(strategyName, label);
        sync(strategyName, Action.MERGE);
    }

    public LoadBalancingStrategy findByName(String strategy) {
        return repository.findByName(strategy);
    }
}
