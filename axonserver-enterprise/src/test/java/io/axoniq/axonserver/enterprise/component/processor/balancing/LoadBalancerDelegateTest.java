package io.axoniq.axonserver.enterprise.component.processor.balancing;

import io.axoniq.axonserver.applicationevents.EventProcessorEvents.EventProcessorStatusUpdated;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.ProcessorStatusRequest;
import io.axoniq.axonserver.component.processor.ClientEventProcessorInfo;
import io.axoniq.axonserver.component.processor.balancing.LoadBalancingOperation;
import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import io.axoniq.axonserver.component.processor.balancing.strategy.ProcessorLoadBalanceStrategy;
import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.listener.ClientProcessors;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.RaftProcessorLoadBalancingService;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import org.junit.*;
import org.springframework.context.ApplicationEventPublisher;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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
    private static final boolean NOT_PROXIED = false;

    private ApplicationEventPublisher eventPublisher;
    private RaftProcessorLoadBalancingService loadBalancingService;
    private ProcessorLoadBalanceStrategy loadBalancingStrategy;

    private LoadBalancerDelegate testSubject;

    @Before
    public void setUp() {
        // Create a single `ClientProcessor` which will match to the used `TrackingEventProcessor`'s context and name
        ClientProcessor testProcessor = new FakeClientProcessor(CLIENT_NAME, BELONGS_TO_CONTEXT, PROCESSOR_NAME);
        List<ClientProcessor> testProcessors = new ArrayList<>();
        testProcessors.add(testProcessor);
        ClientProcessors allClientProcessors = testProcessors::iterator;

        eventPublisher = mock(ApplicationEventPublisher.class);
        // When publishing a `ProcessorStatusRequest`, this will call the testSubject to adjust the count down latch
        doAnswer(invocation -> {
            ProcessorStatusRequest publishedEvent = invocation.getArgument(0);

            EventProcessorInfo eventProcessorInfo = EventProcessorInfo.newBuilder()
                                                                      .setProcessorName(publishedEvent.processorName())
                                                                      .build();
            ClientEventProcessorInfo clientProcessor =
                    new ClientEventProcessorInfo(publishedEvent.clientName(), CONTEXT, eventProcessorInfo);

            testSubject.on(new EventProcessorStatusUpdated(clientProcessor, NOT_PROXIED));
            return null;
        }).when(eventPublisher).publishEvent(isA(ProcessorStatusRequest.class));

        loadBalancingService = mock(RaftProcessorLoadBalancingService.class);
        // Returning an optional of empty will ensure the `LoadBalancerDelegate` uses it's default strategy name
        when(loadBalancingService.findById(any())).thenReturn(Optional.empty());

        loadBalancingStrategy = mock(ProcessorLoadBalanceStrategy.class);

        testSubject = new LoadBalancerDelegate(
                allClientProcessors, eventPublisher, loadBalancingService, loadBalancingStrategy
        );
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

        verify(eventPublisher).publishEvent(isA(ProcessorStatusRequest.class));
        verify(loadBalancingService).findById(testProcessor);
        verify(loadBalancingStrategy).balance(testProcessor, expectedStrategyName);
        verify(mockedLoadBalancingOperation).perform();
    }
}