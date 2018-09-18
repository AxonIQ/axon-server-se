package io.axoniq.axonserver.component.processor.balancing.jpa;

import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;

import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * Created by Sara Pellegrini on 13/08/2018.
 * sara.pellegrini@gmail.com
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

    public TrackingEventProcessor processor() {
        return processor;
    }

    public String strategy() {
        return strategy;
    }

}
