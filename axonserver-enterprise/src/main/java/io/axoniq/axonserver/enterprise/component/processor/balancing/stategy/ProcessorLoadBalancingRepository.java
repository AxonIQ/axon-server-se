package io.axoniq.axonserver.enterprise.component.processor.balancing.stategy;

import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.ProcessorLoadBalancing;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * @author Marc Gathier
 */
@Transactional
public interface ProcessorLoadBalancingRepository extends JpaRepository<ProcessorLoadBalancing, TrackingEventProcessor> {

    List<ProcessorLoadBalancing> findByStrategy(String strategyName);

    @Query("select p from ProcessorLoadBalancing p where p.processor.context = ?1")
    List<ProcessorLoadBalancing> findByContext(String context);

    void deleteAllByProcessorContext(String context);
}
