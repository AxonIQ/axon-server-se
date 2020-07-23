package io.axoniq.axonserver.enterprise.component.processor.balancing.stategy;

import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.AdminProcessorLoadBalancing;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.ReplicationGroupProcessorLoadBalancing;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * JpaRepository for {@link ReplicationGroupProcessorLoadBalancing}
 *
 * @author Marc Gathier
 * @since 4.2
 */
@Transactional
public interface ReplicationGroupProcessorLoadBalancingRepository
        extends JpaRepository<ReplicationGroupProcessorLoadBalancing, TrackingEventProcessor> {

    /**
     * Lists all persisted {@link AdminProcessorLoadBalancing} filtered by the specified context.
     *
     * @param context the context
     */
    @Query("select p from ReplicationGroupProcessorLoadBalancing p where p.processor.context in ?1")
    List<ReplicationGroupProcessorLoadBalancing> findByContext(List<String> context);

    /**
     * Deletes all {@link ReplicationGroupProcessorLoadBalancing} with the specified context.
     *
     * @param context the context
     */
    void deleteAllByProcessorContext(String context);
}
