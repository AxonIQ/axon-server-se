package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.component.processor.balancing.jpa.LoadBalancingStrategy;
import io.axoniq.axonserver.serializer.Printable;

/**
 * Author: marc
 */
public interface LoadBalanceStrategyControllerFacade {

    Iterable<? extends Printable> findAll();

    void save(LoadBalancingStrategy loadBalancingStrategy);

    void delete(String strategyName);

    void updateFactoryBean(String strategyName, String factoryBean);

    void updateLabel(String strategyName, String label);

    LoadBalancingStrategy findByName(String strategyName);
}
