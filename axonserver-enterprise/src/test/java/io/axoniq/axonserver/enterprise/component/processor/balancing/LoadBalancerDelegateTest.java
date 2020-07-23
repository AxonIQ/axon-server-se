package io.axoniq.axonserver.enterprise.component.processor.balancing;

import io.axoniq.axonserver.component.processor.balancing.LoadBalancingOperation;
import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import io.axoniq.axonserver.component.processor.balancing.strategy.ProcessorLoadBalanceStrategy;
import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.ReplicationGroupProcessorLoadBalancingService;
import org.junit.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.mockito.Mockito.*;

/**
 * Test case for the {@link LoadBalancerDelegate}, which tests a single {@link ClientProcessor} load balance operations
 * through mocking and verification.
 *
 * @author Steven van Beelen
 */
public class LoadBalancerDelegateTest {

    private static final String CLIENT_NAME = "clientName";
    private static final String PROCESSOR_NAME = "processorName";
    private static final String CONTEXT = "processorsContext";
    private static final boolean BELONGS_TO_CONTEXT = true;

    private ReplicationGroupProcessorLoadBalancingService loadBalancingService;
    private ProcessorLoadBalanceStrategy loadBalancingStrategy;

    private LoadBalancerDelegate testSubject;

    @Before
    public void setUp() {
        // Create a single `ClientProcessor` which will match to the used `TrackingEventProcessor`'s context and name
        ClientProcessor testProcessor = new FakeClientProcessor(CLIENT_NAME, BELONGS_TO_CONTEXT, PROCESSOR_NAME);
        List<ClientProcessor> testProcessors = new ArrayList<>();
        testProcessors.add(testProcessor);

        loadBalancingService = mock(ReplicationGroupProcessorLoadBalancingService.class);
        // Returning an optional of empty will ensure the `LoadBalancerDelegate` uses it's default strategy name
        when(loadBalancingService.findById(any())).thenReturn(Optional.empty());

        loadBalancingStrategy = mock(ProcessorLoadBalanceStrategy.class);

        testSubject = new LoadBalancerDelegate((context, id) -> completedFuture(null),
                                               loadBalancingService,
                                               loadBalancingStrategy);
    }

    @Test
    public void testBalance() throws InterruptedException {
        String expectedStrategyName = "default";

        TrackingEventProcessor testProcessor = new TrackingEventProcessor(PROCESSOR_NAME, CONTEXT);

        LoadBalancingOperation mockedLoadBalancingOperation = mock(LoadBalancingOperation.class);
        when(loadBalancingStrategy.balance(testProcessor, expectedStrategyName))
                .thenReturn(mockedLoadBalancingOperation);

        testSubject.balance(testProcessor);

        // The LoadBalancerDelegate sleeps 15_000 millis before starting the operation, hence sleep a little over that
        Thread.sleep(15_200);

        verify(loadBalancingService).findById(testProcessor);
        verify(loadBalancingStrategy).balance(testProcessor, expectedStrategyName);
        verify(mockedLoadBalancingOperation).perform();
    }
}