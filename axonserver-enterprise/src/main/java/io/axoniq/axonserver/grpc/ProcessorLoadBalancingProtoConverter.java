package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.ProcessorLoadBalancing;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;

/**
 * Created by Sara Pellegrini on 17/08/2018.
 * sara.pellegrini@gmail.com
 */
public class ProcessorLoadBalancingProtoConverter implements Converter<ProcessorLBStrategy, ProcessorLoadBalancing> {

    @Override
    public ProcessorLoadBalancing map(ProcessorLBStrategy p) {
        String processor = p.getProcessor();
        String component = p.getComponent();
        String context = p.getContext();
        String strategy = p.getStrategy();
        return new ProcessorLoadBalancing(new TrackingEventProcessor(processor, component, context), strategy);
    }

    @Override
    public ProcessorLBStrategy unmap(ProcessorLoadBalancing p) {
        return ProcessorLBStrategy.newBuilder()
                                  .setProcessor(p.processor().name())
                                  .setComponent(p.processor().component())
                                  .setContext(p.processor().context())
                                  .setStrategy(p.strategy())
                                  .build();
    }
}
