package io.axoniq.axonserver.message.query;


import io.axoniq.axonserver.DispatchEvents;
import io.axoniq.axonserver.ProcessingInstructionHelper;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.axonserver.grpc.query.QuerySubscription;
import io.axoniq.axonserver.SubscriptionEvents;
import io.axoniq.axonserver.grpc.query.QueryProviderInbound;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.axonserver.util.CountingStreamObserver;
import io.axoniq.axonserver.util.FailingStreamObserver;
import io.micrometer.core.instrument.Metrics;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.runners.*;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Author: marc
 */
@RunWith(MockitoJUnitRunner.class)
public class QueryDispatcherTest {
    private QueryDispatcher queryDispatcher;

    @Mock
    private QueryRegistrationCache registrationCache;

    @Mock
    private QueryCache queryCache;


    @Before
    public void setup() {
        QueryMetricsRegistry queryMetricsRegistry = new QueryMetricsRegistry(Metrics.globalRegistry, new DefaultMetricCollector());
        queryDispatcher = new QueryDispatcher(registrationCache, queryCache, queryMetricsRegistry);
    }

    @Test
    public void subscribeExpectConfirmation() {
        CountingStreamObserver<QueryProviderInbound> inboundStreamObserver = new CountingStreamObserver<>();

        QueryHandler queryHandler = new DirectQueryHandler(inboundStreamObserver,"client", "componentName");
        queryDispatcher.on(new SubscriptionEvents.SubscribeQuery(Topology.DEFAULT_CONTEXT,
                                                                 QuerySubscription.newBuilder()
                                                                                  .setQuery("test")
                                                                                  .setResultName("testResult")
                                                                                  .setComponentName("testComponent")
                                                                                  .setMessageId("testMessageId")
                                                                                  .setNrOfHandlers(1).build(), queryHandler));
        assertEquals(0, inboundStreamObserver.count );
        verify(registrationCache, times(1)).add(eq(new QueryDefinition(Topology.DEFAULT_CONTEXT, "test")),
                                                any(), any());
    }

    @Test
    public void queryResponse()  {
        AtomicInteger dispatchCalled = new AtomicInteger(0);
        AtomicBoolean doneCalled = new AtomicBoolean(false);
        when(queryCache.get("1234")).thenReturn(new QueryInformation("1234",
                new QueryDefinition("c", QuerySubscription.newBuilder()
                .setQuery("q")
                .setResultName("r")
                .build()), Collections.singleton("client"), 2, r -> dispatchCalled.incrementAndGet(),
                                                                     (client) -> doneCalled.set(true)));
        queryDispatcher.handleResponse(QueryResponse.newBuilder()
                                                                             .setMessageIdentifier("12345")
                                                                                  .setRequestIdentifier("1234")
                                                                             .build(), "client", false);
        verify(queryCache, times(1)).get("1234");
        assertEquals(1, dispatchCalled.get());
        assertFalse(doneCalled.get());

        queryDispatcher.handleResponse(QueryResponse.newBuilder()
                                                                                 .setMessageIdentifier("1234")
                .setRequestIdentifier("1234")
                .build(), "client", false);
        verify(queryCache, times(2)).get("1234");
        assertEquals(2, dispatchCalled.get());
        assertTrue(doneCalled.get());
    }


    @Test
    public void queryNotFound() {
        QueryRequest request = QueryRequest.newBuilder()
                .setQuery("test")
                .setMessageIdentifier("1234")
                .build();
        CountingStreamObserver<QueryResponse> responseObserver = new CountingStreamObserver<>();
        TestResponseObserver testResponseObserver = new TestResponseObserver(responseObserver);
        queryDispatcher.on(new DispatchEvents.DispatchQuery(Topology.DEFAULT_CONTEXT, request,
                                                            testResponseObserver::onNext,
                                                            client-> testResponseObserver.onCompleted(),
                                                            false));
        assertTrue(responseObserver.completed);
        assertEquals(1, responseObserver.count);
        assertNotEquals("", responseObserver.responseList.get(0).getErrorCode());
    }

    @Test
    public void queryFound()  {
        QueryRequest request = QueryRequest.newBuilder()
                .setQuery("test")
                .setMessageIdentifier("1234")
                .build();
        CountingStreamObserver<QueryResponse> responseObserver = new CountingStreamObserver<>();
        Set<QueryHandler> handlers = new HashSet<>();

        CountingStreamObserver<QueryProviderInbound> dispatchStreamObserver = new CountingStreamObserver<>();

        handlers.add(new DirectQueryHandler(dispatchStreamObserver, "client", "componentName"));
        when(registrationCache.find(any(), anyObject())).thenReturn(handlers);
        TestResponseObserver testResponseObserver = new TestResponseObserver(responseObserver);
        queryDispatcher.on(new DispatchEvents.DispatchQuery(Topology.DEFAULT_CONTEXT, request,
                                                            testResponseObserver::onNext,
                                                            client-> testResponseObserver.onCompleted(),
                                                            false));
        assertEquals(0, responseObserver.count);
        //assertEquals(1, dispatchStreamObserver.count);
        verify(queryCache, times(1)).put(any(), any());
    }

    //@Test
    public void queryError() {
        QueryRequest request = QueryRequest.newBuilder()
                .setQuery("test")
                .setMessageIdentifier("1234")
                .build();
        CountingStreamObserver<QueryResponse> responseObserver = new CountingStreamObserver<>();
        Set<QueryHandler> handlers = new HashSet<>();

        handlers.add(new DirectQueryHandler(new FailingStreamObserver<>(), "client", "componentName"));
        when(registrationCache.find(any(), anyObject())).thenReturn(handlers);
        TestResponseObserver testResponseObserver = new TestResponseObserver(responseObserver);
        queryDispatcher.on(new DispatchEvents.DispatchQuery(Topology.DEFAULT_CONTEXT, request,
                                                            testResponseObserver::onNext,
                                                            client-> testResponseObserver.onCompleted(),
                                                            false));
        assertEquals(1, responseObserver.count);
        verify(queryCache, times(1)).put(any(), any());
    }
    @Test
    public void dispatchProxied() {
        QueryRequest request = QueryRequest.newBuilder()
                .setQuery("test")
                .setMessageIdentifier("1234")
                .addProcessingInstructions(ProcessingInstructionHelper.targetClient("client"))
                .build();
        CountingStreamObserver<QueryProviderInbound> countingStreamObserver = new CountingStreamObserver<>();

        QueryHandler handler = new DirectQueryHandler(countingStreamObserver, "client", "componentName");
        when(registrationCache.find(any(), anyObject(), anyObject())).thenReturn(handler);
        queryDispatcher.on(new DispatchEvents.DispatchQuery(null, request, r -> {}, s->{}, true));
        //assertEquals(1, countingStreamObserver.count);
        verify(queryCache, times(1)).put(any(), any());
    }
    @Test
    public void dispatchProxiedNotFound() {
        QueryRequest request = QueryRequest.newBuilder()
                .setQuery("test")
                .setMessageIdentifier("1234")
                .addProcessingInstructions(ProcessingInstructionHelper.targetClient("client"))
                .build();

        AtomicInteger callbackCount = new AtomicInteger(0);

        when(registrationCache.find(anyObject(), anyObject())).thenReturn(null);
        queryDispatcher.on(new DispatchEvents.DispatchQuery(null, request, r -> callbackCount.incrementAndGet(), s->{}, true));
        assertEquals(1, callbackCount.get());
        verify(queryCache, times(0)).put(any(), any());
    }
    //@Test
    public void dispatchProxiedWithError() throws Exception {
        QueryRequest request = QueryRequest.newBuilder()
                .setQuery("test")
                .addProcessingInstructions(ProcessingInstructionHelper.targetClient("client"))
                .setMessageIdentifier("1234")
                .build();
        AtomicInteger dispatchCount = new AtomicInteger(0);
        QueryHandler handler = new DirectQueryHandler(new FailingStreamObserver<>(), "client", "componentName");
        when(registrationCache.find(any(), anyObject(), anyObject())).thenReturn(handler);
        queryDispatcher.on(new DispatchEvents.DispatchQuery(null, request, r -> dispatchCount.incrementAndGet(), s->{}, true));
        queryDispatcher.getQueryQueue().take("client");
        verify(queryCache, times(0)).put(any(), any());
    }


    class TestResponseObserver implements QueryResponseConsumer {
        private final CountingStreamObserver<QueryResponse> responseObserver;

        TestResponseObserver(CountingStreamObserver<QueryResponse> responseObserver) {
            this.responseObserver = responseObserver;
        }

        @Override
        public void onNext(QueryResponse queryResponse) {
            responseObserver.onNext(queryResponse);
        }

        @Override
        public void onCompleted() {
            responseObserver.onCompleted();

        }
    }
}