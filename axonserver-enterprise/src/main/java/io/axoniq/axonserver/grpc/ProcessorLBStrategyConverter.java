package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.BaseProcessorLoadBalancing;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.AdminProcessorLoadBalancing;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.ReplicationGroupProcessorLoadBalancing;
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
                                  .setTokenStoreIdentifier(processorLoadBalancing.processor().tokenStoreIdentifier())
                                  .build();
    }

    /**
     * Converts to a {@link AdminProcessorLoadBalancing} JPA object (used in admin nodes)
     *
     * @param processorLBStrategy the Proto object
     * @return the jpa objecct
     */
    public static AdminProcessorLoadBalancing createJpaProcessorLoadBalancing(ProcessorLBStrategy processorLBStrategy) {
        TrackingEventProcessor processor = new TrackingEventProcessor(processorLBStrategy.getProcessor(),
                                                                      processorLBStrategy.getContext(),
                                                                      processorLBStrategy.getTokenStoreIdentifier());
        return new AdminProcessorLoadBalancing(processor, processorLBStrategy.getStrategy());
    }

    /**
     * Converts to a {@link ReplicationGroupProcessorLoadBalancing} JPA object (used in nodes members of the context)
     *
     * @param processorLBStrategy the Proto object
     * @return the jpa objecct
     */
    public static ReplicationGroupProcessorLoadBalancing createJpaRaftProcessorLoadBalancing(
            ProcessorLBStrategy processorLBStrategy) {
        TrackingEventProcessor processor = new TrackingEventProcessor(processorLBStrategy.getProcessor(),
                                                                      processorLBStrategy.getContext(),
                                                                      processorLBStrategy.getTokenStoreIdentifier());
        return new ReplicationGroupProcessorLoadBalancing(processor, processorLBStrategy.getStrategy());
    }
}
