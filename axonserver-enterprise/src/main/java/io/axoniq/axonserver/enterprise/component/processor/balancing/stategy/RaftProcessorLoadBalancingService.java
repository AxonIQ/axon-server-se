package io.axoniq.axonserver.enterprise.component.processor.balancing.stategy;

import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import io.axoniq.axonserver.enterprise.ContextEvents;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.RaftProcessorLoadBalancing;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

/**
 * Service responsible for the management of {@link RaftProcessorLoadBalancing}s.
 * This implementation uses a JPA repository to save, update and query {@link RaftProcessorLoadBalancing}s.
 *
 * @author Marc Gathier
 * @since 4.2
 */
@Service
public class RaftProcessorLoadBalancingService {

    private final RaftProcessorLoadBalancingRepository repository;

    /**
     * Instantiates a {@link RaftProcessorLoadBalancingService} based on the specified JPA Repository.
     *
     * @param repository the JPA repository for {@link RaftProcessorLoadBalancing}s entities
     */
    public RaftProcessorLoadBalancingService(RaftProcessorLoadBalancingRepository repository) {
        this.repository = repository;
    }

    /**
     * Provides an optional {@link RaftProcessorLoadBalancing} instance if exist for the specified {@link
     * TrackingEventProcessor}.
     *
     * @param processor the tracking event processor
     * @return an optional {@link RaftProcessorLoadBalancing}
     */
    public Optional<RaftProcessorLoadBalancing> findById(TrackingEventProcessor processor) {
        return repository.findById(processor);
    }

    /**
     * Saves or updates the {@link RaftProcessorLoadBalancing}.
     *
     * @param processorLoadBalancing the processor load balancing strategy to persist
     */
    public void save(RaftProcessorLoadBalancing processorLoadBalancing) {
        repository.save(processorLoadBalancing);
    }

    /**
     * Provides the list of {@link RaftProcessorLoadBalancing} for the specified context.
     *
     * @param context the context
     * @return the list of processor load balancing strategies for the context
     */
    public List<RaftProcessorLoadBalancing> findByContext(String context) {
        return repository.findByContext(context);
    }

    /**
     * React on a {@link ContextEvents.ContextDeleted} event by removing all {@link RaftProcessorLoadBalancing} objects
     * for this context.
     *
     * @param contextDeleted context deleted event
     */
    @EventListener
    public void on(ContextEvents.ContextDeleted contextDeleted) {
        repository.deleteAllByProcessorContext(contextDeleted.getContext());
    }
}
