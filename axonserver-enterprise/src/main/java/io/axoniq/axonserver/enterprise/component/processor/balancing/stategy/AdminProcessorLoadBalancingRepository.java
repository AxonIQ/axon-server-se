package io.axoniq.axonserver.enterprise.component.processor.balancing.stategy;

import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.AdminProcessorLoadBalancing;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * JpaRepository for {@link AdminProcessorLoadBalancing}
 *
 * @author Marc Gathier
 */
@Transactional
public interface AdminProcessorLoadBalancingRepository
        extends JpaRepository<AdminProcessorLoadBalancing, TrackingEventProcessor> {

    /**
     * Lists all persisted {@link AdminProcessorLoadBalancing} filtered by the specified strategy.
     *
     * @param strategyName the strategy
     */
    List<AdminProcessorLoadBalancing> findByStrategy(String strategyName);

    /**
     * Lists all persisted {@link AdminProcessorLoadBalancing} filtered by the specified context.
     *
     * @param context the context
     */
    @Query("select p from AdminProcessorLoadBalancing p where p.processor.context = ?1")
    List<AdminProcessorLoadBalancing> findByContext(String context);
}
