package io.axoniq.axonserver.rest;


import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.AdminProcessorLoadBalancing;

import java.util.List;

/**
 * @author Marc Gathier
 */
public interface ProcessorLoadBalancingControllerFacade {

    void save(AdminProcessorLoadBalancing processorLoadBalancing);

    List<AdminProcessorLoadBalancing> findByContext(String context);
}
