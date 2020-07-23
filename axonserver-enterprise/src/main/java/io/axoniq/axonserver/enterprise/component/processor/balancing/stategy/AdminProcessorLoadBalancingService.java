package io.axoniq.axonserver.enterprise.component.processor.balancing.stategy;

import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.AdminProcessorLoadBalancing;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

/**
 * Service responsible for the management of {@link AdminProcessorLoadBalancing}s.
 * This implementation uses a JPA repository to save, update and query {@link AdminProcessorLoadBalancing}s.
 *
 * @author Sara Pellegrini
 */
@Service
public class AdminProcessorLoadBalancingService {

    private final AdminProcessorLoadBalancingRepository repository;

    /**
     * Instantiates a {@link AdminProcessorLoadBalancingService} based on the specified JPA Repository.
     *
     * @param repository the JPA repository for {@link AdminProcessorLoadBalancing}s entities
     */
    public AdminProcessorLoadBalancingService(AdminProcessorLoadBalancingRepository repository) {
        this.repository = repository;
    }

    /**
     * Provides an optional {@link AdminProcessorLoadBalancing} instance if exist for the specified {@link
     * TrackingEventProcessor}.
     *
     * @param processor the tracking event processor
     * @return an optional {@link AdminProcessorLoadBalancing}
     */
    public Optional<AdminProcessorLoadBalancing> findById(TrackingEventProcessor processor) {
        return repository.findById(processor);
    }

    /**
     * Saves or updates the {@link AdminProcessorLoadBalancing}.
     *
     * @param processorLoadBalancing the processor load balancing strategy to persist
     */
    public void save(AdminProcessorLoadBalancing processorLoadBalancing) {
        repository.save(processorLoadBalancing);
    }

    /**
     * Provides the list of {@link AdminProcessorLoadBalancing} for the specified strategy.
     *
     * @param strategyName the strategy
     * @return the list of processor load balancing for the strategy
     */
    List<AdminProcessorLoadBalancing> findByStrategy(String strategyName) {
        return repository.findByStrategy(strategyName);
    }

    /**
     * Provides the list of {@link AdminProcessorLoadBalancing} for the specified context.
     *
     * @param context the context
     * @return the list of processor load balancing strategies for the context
     */
    public List<AdminProcessorLoadBalancing> findByContext(String context) {
        return repository.findByContext(context);
    }

    /**
     * Deletes all {@link AdminProcessorLoadBalancing} for the specified context.
     *
     * @param context the context
     */
    public void deleteByContext(String context) {
        repository.deleteAll(repository.findByContext(context));
    }


    @Transactional
    public void deleteAll() {
        repository.deleteAll();
    }
}
