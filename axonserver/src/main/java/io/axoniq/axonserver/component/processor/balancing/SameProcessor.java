package io.axoniq.axonserver.component.processor.balancing;

import io.axoniq.axonserver.component.processor.listener.ClientProcessor;

import java.util.function.Predicate;

/**
 * Created by Sara Pellegrini on 13/08/2018.
 * sara.pellegrini@gmail.com
 */
public class SameProcessor implements Predicate<ClientProcessor> {

    private final TrackingEventProcessor processor;

    public SameProcessor(TrackingEventProcessor processor) {
        this.processor = processor;
    }

    @Override
    public boolean test(ClientProcessor p) {
        return p.belongsToComponent(processor.component()) &&
                p.belongsToContext(processor.context()) &&
                p.eventProcessorInfo().getProcessorName().equals(processor.name());
    }
}
