package io.axoniq.axonserver.enterprise.component.processor.balancing.stategy;

import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.ProcessorLoadBalancing;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.RaftProcessorLoadBalancing;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * JpaRepository for {@link RaftProcessorLoadBalancing}
 *
 * @author Marc Gathier
 * @since 4.2
 */
@Transactional
public interface RaftProcessorLoadBalancingRepository
        extends JpaRepository<RaftProcessorLoadBalancing, TrackingEventProcessor> {

    /**
     * Lists all persisted {@link ProcessorLoadBalancing} filtered by the specified context.
     *
     * @param context the context
     */
    @Query("select p from RaftProcessorLoadBalancing p where p.processor.context = ?1")
    List<RaftProcessorLoadBalancing> findByContext(String context);

    /**
     * Deletes all {@link RaftProcessorLoadBalancing} with the specified context.
     *
     * @param context the context
     */
    void deleteAllByProcessorContext(String context);
}
