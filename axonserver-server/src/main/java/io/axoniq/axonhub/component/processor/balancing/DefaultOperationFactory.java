package io.axoniq.axonhub.component.processor.balancing;

import io.axoniq.axonhub.component.processor.ApplicationProcessorEventsSource;
import io.axoniq.axonhub.component.processor.listener.ClientProcessors;

import static java.util.stream.StreamSupport.stream;

/**
 * Created by Sara Pellegrini on 08/08/2018.
 * sara.pellegrini@gmail.com
 */
public class DefaultOperationFactory implements OperationFactory {

    private final ApplicationProcessorEventsSource processorEventsSource;

    private final ClientProcessors processors;

    public DefaultOperationFactory(
            ApplicationProcessorEventsSource processorEventsSource,
            ClientProcessors processors) {
        this.processorEventsSource = processorEventsSource;
        this.processors = processors;
    }

    @Override
    public LoadBalancingOperation move(Integer segment, TrackingEventProcessor processor, String source,
                                       String target) {
        return new Move(processor, target, segment);
    }

    private class Move implements LoadBalancingOperation {

        private final TrackingEventProcessor processor;
        private final String target;
        private final Integer segment;

        private Move(TrackingEventProcessor processor, String target, Integer segment) {
            this.processor = processor;
            this.target = target;
            this.segment = segment;
        }

        @Override
        public void perform() {
            stream(processors.spliterator(), false)
                    .filter(new SameProcessor(processor))
                    .filter(p -> !target.equals(p.clientId()))
                    .forEach(p -> processorEventsSource.releaseSegment(p.clientId(), processor.name(), segment));
        }

        @Override
        public String toString() {
            return "Move segment "+ segment+ " to client "+ target;
        }
    }

}
