package io.axoniq.axonserver.enterprise.component.processor.balancing.stategy;

import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import io.axoniq.axonserver.enterprise.ContextEvents;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.ReplicationGroupProcessorLoadBalancing;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

/**
 * Service responsible for the management of {@link ReplicationGroupProcessorLoadBalancing}s.
 * This implementation uses a JPA repository to save, update and query {@link ReplicationGroupProcessorLoadBalancing}s.
 *
 * @author Marc Gathier
 * @since 4.2
 */
@Service
public class ReplicationGroupProcessorLoadBalancingService {

    private final ReplicationGroupProcessorLoadBalancingRepository repository;

    /**
     * Instantiates a {@link ReplicationGroupProcessorLoadBalancingService} based on the specified JPA Repository.
     *
     * @param repository the JPA repository for {@link ReplicationGroupProcessorLoadBalancing}s entities
     */
    public ReplicationGroupProcessorLoadBalancingService(ReplicationGroupProcessorLoadBalancingRepository repository) {
        this.repository = repository;
    }

    /**
     * Provides an optional {@link ReplicationGroupProcessorLoadBalancing} instance if exist for the specified {@link
     * TrackingEventProcessor}.
     *
     * @param processor the tracking event processor
     * @return an optional {@link ReplicationGroupProcessorLoadBalancing}
     */
    public Optional<ReplicationGroupProcessorLoadBalancing> findById(TrackingEventProcessor processor) {
        return repository.findById(processor);
    }

    /**
     * Saves or updates the {@link ReplicationGroupProcessorLoadBalancing}.
     *
     * @param processorLoadBalancing the processor load balancing strategy to persist
     */
    public void save(ReplicationGroupProcessorLoadBalancing processorLoadBalancing) {
        repository.save(processorLoadBalancing);
    }

    /**
     * Provides the list of {@link ReplicationGroupProcessorLoadBalancing} for the specified context.
     *
     * @param context the context
     * @return the list of processor load balancing strategies for the context
     */
    public List<ReplicationGroupProcessorLoadBalancing> findByContext(List<String> context) {
        return repository.findByContext(context);
    }

    /**
     * React on a {@link ContextEvents.ContextDeleted} event by removing all {@link
     * ReplicationGroupProcessorLoadBalancing} objects
     * for this context.
     *
     * @param contextDeleted context deleted event
     */
    @EventListener
    public void on(ContextEvents.ContextDeleted contextDeleted) {
        repository.deleteAllByProcessorContext(contextDeleted.context());
    }
}
