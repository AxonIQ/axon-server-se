package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.component.processor.balancing.jpa.LoadBalancingStrategy;
import io.axoniq.axonserver.grpc.internal.LoadBalanceStrategy;

/**
 * Convert between JPA {@link LoadBalancingStrategy} object and Protobuf {@link LoadBalanceStrategy} object and vice versa.
 * @author Marc Gathier
 */
public class LoadBalancingStrategyConverter {
    public static LoadBalanceStrategy createLoadBalanceStrategy(LoadBalancingStrategy loadBalancingStrategy) {
        return LoadBalanceStrategy.newBuilder()
                                  .setName(loadBalancingStrategy.name())
                                  .setFactoryBean(loadBalancingStrategy.factoryBean())
                                  .setLabel(loadBalancingStrategy.label())
                                  .build();
    }

    public static LoadBalancingStrategy createJpaLoadBalancingStrategy(LoadBalanceStrategy loadBalanceStrategy) {
        return new LoadBalancingStrategy(loadBalanceStrategy.getName(),
                                         loadBalanceStrategy.getLabel(),
                                         loadBalanceStrategy.getFactoryBean());
    }

}
