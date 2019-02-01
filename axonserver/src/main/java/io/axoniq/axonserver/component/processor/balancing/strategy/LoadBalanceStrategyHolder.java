package io.axoniq.axonserver.component.processor.balancing.strategy;

import io.axoniq.axonserver.component.processor.balancing.jpa.LoadBalancingStrategy;
import io.axoniq.axonserver.serializer.Printable;

import java.util.Set;

/**
 * Author: marc
 */
public interface LoadBalanceStrategyHolder {

    LoadBalancingStrategy findByName(String strategyName);

    Iterable<? extends Printable> findAll();

    Set<String> getFactoryBeans();
}
