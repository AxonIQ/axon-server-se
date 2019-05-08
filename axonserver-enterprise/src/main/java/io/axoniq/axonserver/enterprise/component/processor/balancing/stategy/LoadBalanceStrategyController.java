package io.axoniq.axonserver.enterprise.component.processor.balancing.stategy;

import io.axoniq.axonserver.component.processor.balancing.jpa.LoadBalancingStrategy;
import io.axoniq.axonserver.component.processor.balancing.strategy.LoadBalanceStrategyHolder;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.ProcessorLoadBalancing;
import org.springframework.context.annotation.Primary;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by Sara Pellegrini on 16/08/2018.
 * sara.pellegrini@gmail.com
 */
@Component
@Primary
public class LoadBalanceStrategyController implements LoadBalanceStrategyHolder {

    private final ProcessorLoadBalancingService processorLoadBalancingService;

    private final LoadBalanceStrategyRepository repository;

    public LoadBalanceStrategyController(
            ProcessorLoadBalancingService processorLoadBalancingService,
            LoadBalanceStrategyRepository repository) {
        this.processorLoadBalancingService = processorLoadBalancingService;
        this.repository = repository;
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

    @Override
    public Set<String> getFactoryBeans() {
        return repository.findAll().stream().map(LoadBalancingStrategy::factoryBean).collect(Collectors.toSet());
    }

    @Override
    public Iterable<LoadBalancingStrategy> findAll() {
        return repository.findAll(new Sort(Sort.DEFAULT_DIRECTION,"id"));
    }

    @Transactional
    public void delete(String strategyName) {
        if ("default".equals(strategyName)){
            throw new IllegalArgumentException("Cannot delete default load balancing strategy.");
        }

        List<ProcessorLoadBalancing> processors = processorLoadBalancingService.findByStrategy(strategyName);
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

    @Override
    public LoadBalancingStrategy findByName(String strategy) {
        return repository.findByName(strategy);
    }
}
