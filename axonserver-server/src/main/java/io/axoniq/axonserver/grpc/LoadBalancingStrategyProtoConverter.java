package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.component.processor.balancing.jpa.LoadBalancingStrategy;
import io.axoniq.axonserver.internal.grpc.LoadBalanceStrategy;

/**
 * Created by Sara Pellegrini on 17/08/2018.
 * sara.pellegrini@gmail.com
 */
public class LoadBalancingStrategyProtoConverter implements Converter<LoadBalanceStrategy, LoadBalancingStrategy> {

    @Override
    public LoadBalancingStrategy map(LoadBalanceStrategy strategy) {
        return new LoadBalancingStrategy(strategy.getName(), strategy.getLabel(), strategy.getFactoryBean());
    }

    @Override
    public LoadBalanceStrategy unmap(LoadBalancingStrategy strategy) {
        return LoadBalanceStrategy.newBuilder()
                                  .setName(strategy.name())
                                  .setLabel(strategy.label())
                                  .setFactoryBean(strategy.factoryBean()).build();
    }
}
