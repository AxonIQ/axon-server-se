package io.axoniq.axonserver.enterprise.component.processor.balancing.jpa;

import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;

import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * This entity represents the strategy to be used for auto-balancing a tracking event processor.
 * The primary key is semantic key used to uniquely define the event processor, while the strategy
 * is the unique identifier need to define the strategy to be used.
 *
 * @author Sara Pellegrini
 */
@Entity
public class ProcessorLoadBalancing  {

    @SuppressWarnings("unused")
    public ProcessorLoadBalancing() {
    }

    public ProcessorLoadBalancing(TrackingEventProcessor processor,
                                  String strategy) {
        this.processor = processor;
        this.strategy = strategy;
    }

    @Embedded @Id
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
     * @return the strategy
     */
    public String strategy() {
        return strategy;
    }

}
