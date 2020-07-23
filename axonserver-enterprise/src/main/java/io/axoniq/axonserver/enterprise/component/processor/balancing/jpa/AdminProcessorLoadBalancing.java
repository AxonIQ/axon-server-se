package io.axoniq.axonserver.enterprise.component.processor.balancing.jpa;

import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;

import javax.persistence.Entity;
import javax.persistence.Table;

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
@Table(name = "adm_processor_load_balancing")
public class AdminProcessorLoadBalancing extends BaseProcessorLoadBalancing {

    @SuppressWarnings("unused")
    public AdminProcessorLoadBalancing() {
    }

    public AdminProcessorLoadBalancing(TrackingEventProcessor processor, String strategy) {
        super(processor, strategy);
    }
}
