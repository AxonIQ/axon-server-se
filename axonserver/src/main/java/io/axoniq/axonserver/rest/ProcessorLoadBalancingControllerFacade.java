package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.component.processor.balancing.jpa.ProcessorLoadBalancing;

import java.util.List;

/**
 * Author: marc
 */
public interface ProcessorLoadBalancingControllerFacade {

    void save(ProcessorLoadBalancing processorLoadBalancing);

    List<ProcessorLoadBalancing> findByComponentAndContext(String component, String context);
}
