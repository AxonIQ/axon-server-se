package io.axoniq.axonserver.component.processor.balancing.strategy;

import io.axoniq.axonserver.component.processor.balancing.LoadBalancingOperation;
import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.LoadBalanceStrategyRepository;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.LoadBalancingStrategy;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.ProcessorLoadBalancing;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.ProcessorLoadBalancingRepository;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Created by Sara Pellegrini on 13/08/2018.
 * sara.pellegrini@gmail.com
 */
@Component
public class ProcessorLoadBalanceStrategy implements
        io.axoniq.axonserver.component.processor.balancing.LoadBalancingStrategy {

    private final ProcessorLoadBalancingRepository repository;

    private final LoadBalanceStrategyRepository strategyRepository;

    private final Map<String, Factory> factories;


    public ProcessorLoadBalanceStrategy(
            ProcessorLoadBalancingRepository repository,
            LoadBalanceStrategyRepository strategyRepository,
            Map<String, Factory> factories) {
        this.strategyRepository = strategyRepository;
        this.repository = repository;
        this.factories = factories;
    }

    @Override
    public LoadBalancingOperation balance(TrackingEventProcessor processor) {
        String strategyName = repository.findById(processor)
                                        .map(ProcessorLoadBalancing::strategy)
                                        .orElse("default");
        LoadBalancingStrategy strategy =  strategyRepository.findByName(strategyName);
        Factory factory = factories.getOrDefault(strategy.factoryBean(), NoLoadBalanceStrategy::new);
        return factory.create().balance(processor);
    }

}
