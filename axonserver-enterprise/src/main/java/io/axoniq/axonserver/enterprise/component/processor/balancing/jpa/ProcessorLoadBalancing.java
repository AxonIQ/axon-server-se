package io.axoniq.axonserver.enterprise.component.processor.balancing.jpa;

import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;

import javax.persistence.Entity;

/**
 * This entity represents the strategy to be used for auto-balancing a tracking event processor.
 * The primary key is semantic key used to uniquely define the event processor, while the strategy
 * is the unique identifier needed to define the strategy to be used.
 * <p>
 * This is an ADMIN node specific entity.
 *
 *
 * @author Sara Pellegrini
 * @since 4.0
 */
@Entity
public class ProcessorLoadBalancing extends BaseProcessorLoadBalancing {

    @SuppressWarnings("unused")
    public ProcessorLoadBalancing() {
    }

    public ProcessorLoadBalancing(TrackingEventProcessor processor, String strategy) {
        super(processor, strategy);
    }
}
