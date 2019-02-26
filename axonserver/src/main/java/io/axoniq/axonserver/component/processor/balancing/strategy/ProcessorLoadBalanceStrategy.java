package io.axoniq.axonserver.component.processor.balancing.strategy;

import io.axoniq.axonserver.component.processor.balancing.LoadBalancingOperation;
import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import io.axoniq.axonserver.component.processor.balancing.jpa.LoadBalancingStrategy;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Created by Sara Pellegrini on 13/08/2018.
 * sara.pellegrini@gmail.com
 */
@Component
public class ProcessorLoadBalanceStrategy  {


    private final LoadBalanceStrategyHolder strategyRepository;

    private final Map<String, io.axoniq.axonserver.component.processor.balancing.LoadBalancingStrategy.Factory> factories;


    public ProcessorLoadBalanceStrategy(
            LoadBalanceStrategyHolder strategyRepository,
            Map<String, io.axoniq.axonserver.component.processor.balancing.LoadBalancingStrategy.Factory> factories) {
        this.strategyRepository = strategyRepository;
        this.factories = factories;
    }

    public LoadBalancingOperation balance(TrackingEventProcessor processor, String strategyName) {
//        String strategyName = repository.findById(processor)
//                                        .map(ProcessorLoadBalancing::strategy)
//                                        .orElse("default");
        LoadBalancingStrategy strategy =  strategyRepository.findByName(strategyName);
        io.axoniq.axonserver.component.processor.balancing.LoadBalancingStrategy.Factory factory = factories.getOrDefault(strategy.factoryBean(), NoLoadBalanceStrategy::new);
        return factory.create().balance(processor);
    }

}
