package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.ProcessorLoadBalancing;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;

/**
 * @author Marc Gathier
 */
public class ProcessorLBStrategyConverter {
    public static ProcessorLBStrategy createProcessorLBStrategy(ProcessorLoadBalancing processorLoadBalancing) {
        return ProcessorLBStrategy.newBuilder()
                                  .setStrategy(processorLoadBalancing.strategy())
                                  .setContext(processorLoadBalancing.processor().context())
                                  .setProcessor(processorLoadBalancing.processor().name())
                                  .setComponent(processorLoadBalancing.processor().component())
                                  .build();
    }

    public static ProcessorLoadBalancing createJpaProcessorLoadBalancing(ProcessorLBStrategy processorLBStrategy) {
        TrackingEventProcessor processor = new TrackingEventProcessor(processorLBStrategy.getProcessor(),
                                                                      processorLBStrategy.getComponent(),
                                                                      processorLBStrategy.getContext());
        return new ProcessorLoadBalancing(processor, processorLBStrategy.getStrategy());
    }

}
