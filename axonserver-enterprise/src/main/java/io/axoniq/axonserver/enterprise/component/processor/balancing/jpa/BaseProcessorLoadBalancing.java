package io.axoniq.axonserver.enterprise.component.processor.balancing.jpa;

import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;

import javax.persistence.Embedded;
import javax.persistence.Id;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.MappedSuperclass;

/**
 * Base class for saving the load balancing strategy per processor.
 *
 * @author Marc Gathier
 * @since 4.2
 */
@MappedSuperclass
@Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
public abstract class BaseProcessorLoadBalancing {

    @SuppressWarnings("unused")
    public BaseProcessorLoadBalancing() {
    }

    public BaseProcessorLoadBalancing(TrackingEventProcessor processor,
                                      String strategy) {
        this.processor = processor;
        this.strategy = strategy;
    }

    @Embedded
    @Id
    private TrackingEventProcessor processor;

    private String strategy;

    /**
     * Returns the tracking event processor, identified by context and processor name.
     *
     * @return the tracking event processor
     */
    public TrackingEventProcessor processor() {
        return processor;
    }

    /**
     * Returns the strategy set for this Tracking Event Processor auto balancing operations.
     *
     * @return the strategy
     */
    public String strategy() {
        return strategy;
    }
}
