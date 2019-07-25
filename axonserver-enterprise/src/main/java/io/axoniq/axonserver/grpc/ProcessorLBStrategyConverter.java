package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.BaseProcessorLoadBalancing;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.ProcessorLoadBalancing;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.RaftProcessorLoadBalancing;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;

/**
 * Converts JPA {@link BaseProcessorLoadBalancing} object to {@link ProcessorLBStrategy} proto object and vice-versa
 * @author Marc Gathier
 * @since 4.0
 */
public class ProcessorLBStrategyConverter {

    public static ProcessorLBStrategy createProcessorLBStrategy(BaseProcessorLoadBalancing processorLoadBalancing) {
        return ProcessorLBStrategy.newBuilder()
                                  .setStrategy(processorLoadBalancing.strategy())
                                  .setContext(processorLoadBalancing.processor().context())
                                  .setProcessor(processorLoadBalancing.processor().name())
                                  .build();
    }

    /**
     * Converts to a {@link ProcessorLoadBalancing} JPA object (used in admin nodes)
     *
     * @param processorLBStrategy the Proto object
     * @return the jpa objecct
     */
    public static ProcessorLoadBalancing createJpaProcessorLoadBalancing(ProcessorLBStrategy processorLBStrategy) {
        TrackingEventProcessor processor = new TrackingEventProcessor(processorLBStrategy.getProcessor(),
                                                                      processorLBStrategy.getContext());
        return new ProcessorLoadBalancing(processor, processorLBStrategy.getStrategy());
    }

    /**
     * Converts to a {@link RaftProcessorLoadBalancing} JPA object (used in nodes members of the context)
     *
     * @param processorLBStrategy the Proto object
     * @return the jpa objecct
     */
    public static RaftProcessorLoadBalancing createJpaRaftProcessorLoadBalancing(
            ProcessorLBStrategy processorLBStrategy) {
        TrackingEventProcessor processor = new TrackingEventProcessor(processorLBStrategy.getProcessor(),
                                                                      processorLBStrategy.getContext());
        return new RaftProcessorLoadBalancing(processor, processorLBStrategy.getStrategy());
    }
}
