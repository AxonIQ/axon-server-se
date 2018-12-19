package io.axoniq.axonserver.component.processor.balancing.jpa;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Created by Sara Pellegrini on 16/08/2018.
 * sara.pellegrini@gmail.com
 */
@Component
public class LoadBalanceStrategyController {

    private final ProcessorLoadBalancingController processorLoadBalancingController;

    private final LoadBalanceStrategyRepository repository;

    private final ApplicationEventPublisher eventPublisher;

    public LoadBalanceStrategyController(
            ProcessorLoadBalancingController processorLoadBalancingController,
            LoadBalanceStrategyRepository repository,
            ApplicationEventPublisher eventPublisher) {
        this.processorLoadBalancingController = processorLoadBalancingController;
        this.repository = repository;
        this.eventPublisher = eventPublisher;
    }

    @Transactional
    public void save(LoadBalancingStrategy strategy){
        synchronized (repository) {
            LoadBalancingStrategy existing = repository.findByName(strategy.name());
            if( existing != null) {
                repository.updateFactoryBean(strategy.name(), strategy.factoryBean());
                repository.updateLabel(strategy.name(), strategy.label());
            } else {
                repository.saveAndFlush(strategy);
            }
        }
    }


    public Iterable<LoadBalancingStrategy> findAll() {
        return repository.findAll(new Sort("id"));
    }

    @Transactional
    public void delete(String strategyName) {
        if ("default".equals(strategyName)){
            throw new IllegalArgumentException("Cannot delete default load balancing strategy.");
        }

        List<ProcessorLoadBalancing> processors = processorLoadBalancingController.findByStrategy(strategyName);
        if (processors.size() > 0){
            throw new IllegalStateException("Impossible to delete " + strategyName + " because is currently used.");
        }

        synchronized (repository) {
            repository.deleteByName(strategyName);
            repository.flush();
        }
    }

    public void updateFactoryBean(String strategyName, String factoryBean) {
        repository.updateFactoryBean(strategyName, factoryBean);
    }

    public void updateLabel(String strategyName, String label) {
        repository.updateLabel(strategyName, label);
    }

    public LoadBalancingStrategy findByName(String strategy) {
        return repository.findByName(strategy);
    }
}
