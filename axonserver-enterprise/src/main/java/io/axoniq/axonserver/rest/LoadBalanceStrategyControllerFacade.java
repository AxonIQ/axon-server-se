package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.component.processor.balancing.jpa.LoadBalancingStrategy;
import io.axoniq.axonserver.serializer.Printable;

/**
 * @author Marc Gathier
 */
public interface LoadBalanceStrategyControllerFacade {

    Iterable<? extends Printable> findAll();

    LoadBalancingStrategy findByName(String strategyName);
}
