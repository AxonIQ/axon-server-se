package io.axoniq.axonserver.enterprise.component.processor.balancing.stategy;

import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.ProcessorLoadBalancing;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

/**
 * Service responsible for the management of {@link ProcessorLoadBalancing}s.
 * This implementation uses a JPA repository to save, update and query {@link ProcessorLoadBalancing}s.
 *
 * @author Sara Pellegrini
 */
@Service
public class ProcessorLoadBalancingService {

    private final ProcessorLoadBalancingRepository repository;

    /**
     * Instantiates a {@link ProcessorLoadBalancingService} based on the specified JPA Repository.
     *
     * @param repository the JPA repository for {@link ProcessorLoadBalancing}s entities
     */
    public ProcessorLoadBalancingService(ProcessorLoadBalancingRepository repository) {
        this.repository = repository;
    }

    /**
     * Provides an optional {@link ProcessorLoadBalancing} instance if exist for the specified {@link
     * TrackingEventProcessor}.
     *
     * @param processor the tracking event processor
     * @return an optional {@link ProcessorLoadBalancing}
     */
    public Optional<ProcessorLoadBalancing> findById(TrackingEventProcessor processor) {
        return repository.findById(processor);
    }

    /**
     * Saves or updates the {@link ProcessorLoadBalancing}.
     
     * @param processorLoadBalancing the processor load balancing strategy to persist
     */
    public void save(ProcessorLoadBalancing processorLoadBalancing){
        repository.save(processorLoadBalancing);
    }

    /**
     * Provides the list of {@link ProcessorLoadBalancing} for the specified strategy.
     
     * @param strategyName the strategy
     * @return the list of processor load balancing for the strategy
     */
    List<ProcessorLoadBalancing> findByStrategy(String strategyName) {
        return repository.findByStrategy(strategyName);
    }

    /**
     * Provides the list of {@link ProcessorLoadBalancing} for the specified context.
     *
     * @param context the context
     * @return the list of processor load balancing strategies for the context
     */
    public List<ProcessorLoadBalancing> findByContext(String context) {
        return repository.findByContext(context);
    }
}
