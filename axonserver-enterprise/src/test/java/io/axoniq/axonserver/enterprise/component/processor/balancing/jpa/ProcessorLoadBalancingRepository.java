package io.axoniq.axonserver.enterprise.component.processor.balancing.jpa;

import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

/**
 * Created by Sara Pellegrini on 13/08/2018.
 * sara.pellegrini@gmail.com
 */
public interface ProcessorLoadBalancingRepository extends JpaRepository<ProcessorLoadBalancing, TrackingEventProcessor> {

    @Query("select p from ProcessorLoadBalancing p where p.processor.component = ?1 and p.processor.context = ?2")
    List<ProcessorLoadBalancing> findByComponentAndContext(String component, String context);

    List<ProcessorLoadBalancing> findByStrategy(String strategyName);
}
