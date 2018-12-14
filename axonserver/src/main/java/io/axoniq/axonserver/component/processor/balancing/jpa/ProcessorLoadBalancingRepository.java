package io.axoniq.axonserver.component.processor.balancing.jpa;

import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

/**
 * Author: marc
 */
public interface ProcessorLoadBalancingRepository extends JpaRepository<ProcessorLoadBalancing, TrackingEventProcessor> {

    List<ProcessorLoadBalancing> findByStrategy(String strategyName);

    @Query("select p from ProcessorLoadBalancing p where p.processor.component = ?1 and p.processor.context = ?2")
    List<ProcessorLoadBalancing> findByComponentAndContext(String component, String context);

    @Query("select p from ProcessorLoadBalancing p where p.processor.context = ?1")
    List<ProcessorLoadBalancing> findByContext(String context);
}
