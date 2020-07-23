package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.AdminProcessorLoadBalancing;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;
import org.junit.*;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link ProcessorLBStrategyConverter}
 *
 * @author Sara Pellegrini
 */
public class ProcessorLBStrategyConverterTest {

    @Test
    public void createProcessorLBStrategy() {
        TrackingEventProcessor processor = new TrackingEventProcessor("P", "C", "T");
        AdminProcessorLoadBalancing entity = new AdminProcessorLoadBalancing(processor, "S");
        ProcessorLBStrategy processorLBStrategy = ProcessorLBStrategyConverter.createProcessorLBStrategy(entity);
        assertEquals(ProcessorLBStrategy.newBuilder()
                                        .setContext("C")
                                        .setStrategy("S")
                                        .setProcessor("P")
                                        .setTokenStoreIdentifier("T")
                                        .build(), processorLBStrategy);
    }

    @Test
    public void createJpaProcessorLoadBalancing() {
        ProcessorLBStrategy o = ProcessorLBStrategy.newBuilder()
                                                   .setContext("C")
                                                   .setStrategy("S")
                                                   .setProcessor("P")
                                                   .setTokenStoreIdentifier("T")
                                                   .build();
        AdminProcessorLoadBalancing entity = ProcessorLBStrategyConverter.createJpaProcessorLoadBalancing(o);
        assertEquals(entity.processor().name(), "P");
        assertEquals(entity.processor().context(), "C");
        assertEquals(entity.processor().tokenStoreIdentifier(), "T");
        assertEquals(entity.strategy(), "S");
    }

    @Test
    public void createJpaRaftProcessorLoadBalancing() {
        ProcessorLBStrategy o = ProcessorLBStrategy.newBuilder()
                                                   .setContext("C")
                                                   .setStrategy("S")
                                                   .setProcessor("P")
                                                   .setTokenStoreIdentifier("T")
                                                   .build();
        AdminProcessorLoadBalancing entity = ProcessorLBStrategyConverter.createJpaProcessorLoadBalancing(o);
        assertEquals(entity.processor().name(), "P");
        assertEquals(entity.processor().context(), "C");
        assertEquals(entity.processor().tokenStoreIdentifier(), "T");
        assertEquals(entity.strategy(), "S");
    }
}