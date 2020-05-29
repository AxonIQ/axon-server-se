package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.applicationevents.EventProcessorEvents.MergeSegmentRequest;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.SplitSegmentRequest;
import io.axoniq.axonserver.applicationevents.SubscriptionEvents;
import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.component.tags.ClientTagsUpdate;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.licensing.LicenseManager;
import io.axoniq.axonserver.grpc.internal.*;
import io.axoniq.axonserver.grpc.query.QuerySubscription;
import io.axoniq.axonserver.message.command.CommandDispatcher;
import io.axoniq.axonserver.message.query.QueryDispatcher;
import io.axoniq.axonserver.spring.FakeApplicationEventPublisher;
import io.axoniq.axonserver.test.FakeStreamObserver;
import io.axoniq.axonserver.topology.Topology;
import io.grpc.stub.StreamObserver;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;

import static io.axoniq.axonserver.grpc.internal.ConnectorResponse.ResponseCase.CONNECT_RESPONSE;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 * @author Marc Gathier
 */
public class MessagingClusterServiceTest {

    private final CommandDispatcher commandDispatcher = mock(CommandDispatcher.class);
    private final QueryDispatcher queryDispatcher = mock(QueryDispatcher.class);
    private final ClusterController clusterController = mock(ClusterController.class);
    private final FakeApplicationEventPublisher eventPublisher = new FakeApplicationEventPublisher();
    private final LicenseManager licenseManager = mock(LicenseManager.class);
    private MessagingClusterService testSubject;

    @Before
    public void setUp() {
        testSubject = new MessagingClusterService(
                commandDispatcher, queryDispatcher, clusterController, null, eventPublisher
        );

        eventPublisher.add(event -> {
            if (event instanceof TopologyEvents.ApplicationConnected) {
                testSubject.on((TopologyEvents.ApplicationConnected) event);
            }
            if (event instanceof TopologyEvents.ApplicationDisconnected) {
                testSubject.on((TopologyEvents.ApplicationDisconnected) event);
            }
        });
    }

    @Test
    public void connect() {
        FakeStreamObserver<ConnectorResponse> responseStream = new FakeStreamObserver<>();
        StreamObserver<ConnectorCommand> requestStream = testSubject.openStream(responseStream);
        requestStream.onNext(ConnectorCommand.newBuilder().setConnect(
                ConnectRequest.newBuilder().setNodeInfo(NodeInfo.newBuilder().setNodeName("application-server1"))
        ).build());
        assertEquals(1, responseStream.values().size()); // connect response
        assertEquals(CONNECT_RESPONSE, responseStream.values().get(0).getResponseCase());
    }

    @Test
    public void subscribeQuery() {
        FakeStreamObserver<ConnectorResponse> responseStream = new FakeStreamObserver<>();

        InternalQuerySubscription testMessage =
                InternalQuerySubscription.newBuilder()
                                         .setQuery(QuerySubscription.newBuilder()
                                                                    .setQuery("query")
                                                                    .setComponentName("Component")
                                                                    .setClientId("Client")
                                                                    .setResultName("Result"))
                                         .setContext(Topology.DEFAULT_CONTEXT)
                                         .build();

        StreamObserver<ConnectorCommand> requestStream = testSubject.openStream(responseStream);
        requestStream.onNext(ConnectorCommand.newBuilder().setConnect(ConnectRequest.newBuilder()
                                                                                    .setNodeInfo(NodeInfo.newBuilder()
                                                                                                         .setNodeName("node-1")
                                                                                                         )
        ).build());

        requestStream.onNext(ConnectorCommand.newBuilder().setSubscribeQuery(testMessage).build());
        assertEquals(1, responseStream.values().size());

        requestStream.onCompleted();
        Iterator<Object> eventIterator = eventPublisher.events().iterator();
        Object next = eventIterator.next();
        assertEquals(SubscriptionEvents.SubscribeQuery.class, next.getClass());
        next = eventIterator.next();
        assertEquals(ClusterEvents.AxonServerInstanceDisconnected.class, next.getClass());
        assertFalse(eventIterator.hasNext());
    }

    @Test
    public void connectDisconnect() {
        FakeStreamObserver<ConnectorResponse> responseStream = new FakeStreamObserver<>();
        StreamObserver<ConnectorCommand> requestStream = testSubject.openStream(responseStream);
        requestStream.onNext(ConnectorCommand.newBuilder().setConnect(ConnectRequest.newBuilder()
                                                                                    .setNodeInfo(NodeInfo.newBuilder()
                                                                                                         .setNodeName("node-1")
                                                                                    )
        ).build());

        requestStream.onNext(ConnectorCommand.newBuilder()
                                             .setClientStatus(ClientStatus.newBuilder()
                                                              .setClientName("client1")
                                                              .setComponentName("demoComponent")
                                                              .setContext("demo")
                                                              .setConnected(true)
                                             )
                                             .build());
        testSubject.on(new TopologyEvents.ApplicationConnected("demo", "demoComponent", "client1"));
        requestStream.onNext(ConnectorCommand.newBuilder()
                                             .setClientStatus(ClientStatus.newBuilder()
                                                                          .setClientName("client1")
                                                                          .setComponentName("demoComponent")
                                                                          .setContext("demo")
                                                                          .setConnected(false)
                                             )
                                             .build());

        Iterator<Object> eventIterator = eventPublisher.events().iterator();
        Object next = eventIterator.next();
        assertEquals(ClientTagsUpdate.class, next.getClass());
        assertEquals(ClientTagsUpdate.class, next.getClass());
        assertTrue(eventIterator.hasNext());
        next = eventIterator.next();
        assertEquals(TopologyEvents.ApplicationConnected.class, next.getClass());
        assertFalse(eventIterator.hasNext());
    }

    @Test
    public void queryFlowControl() {
        InternalFlowControl testMessage = InternalFlowControl.newBuilder()
                                                             .setGroup(Group.QUERY)
                                                             .setNodeName("node1")
                                                             .setPermits(1000)
                                                             .build();
        FakeStreamObserver<ConnectorResponse> responseStream = new FakeStreamObserver<>();
        StreamObserver<ConnectorCommand> requestStream = testSubject.openStream(responseStream);

        requestStream.onNext(ConnectorCommand.newBuilder().setFlowControl(testMessage).build());

        assertEquals(0, responseStream.values().size());
        requestStream.onCompleted();
    }

    @Test
    public void commandFlowControl() {
        InternalFlowControl testMessage = InternalFlowControl.newBuilder()
                                                             .setGroup(Group.COMMAND)
                                                             .setNodeName("node1")
                                                             .setPermits(1000)
                                                             .build();

        FakeStreamObserver<ConnectorResponse> responseStream = new FakeStreamObserver<>();
        StreamObserver<ConnectorCommand> requestStream = testSubject.openStream(responseStream);

        requestStream.onNext(ConnectorCommand.newBuilder().setFlowControl(testMessage).build());

        assertEquals(0, responseStream.values().size());
        requestStream.onCompleted();
    }

    @Test
    public void testSplitSegmentConnectorCommandComingIn() {
        String expectedClientName = "clientName";
        String expectedProcessorName = "processorName";
        int expectedSegmentId = 1;
        ClientEventProcessorSegment testSplitMessage =
                ClientEventProcessorSegment.newBuilder()
                                           .setClient(expectedClientName)
                                           .setProcessorName(expectedProcessorName)
                                           .setSegmentIdentifier(expectedSegmentId)
                                           .build();

        FakeStreamObserver<ConnectorResponse> responseStream = new FakeStreamObserver<>();
        StreamObserver<ConnectorCommand> requestStream = testSubject.openStream(responseStream);

        requestStream.onNext(ConnectorCommand.newBuilder().setSplitSegment(testSplitMessage).build());

        assertEquals(0, responseStream.values().size());
        requestStream.onCompleted();

        Iterator<Object> publishedEvents = eventPublisher.events().iterator();
        assertTrue(publishedEvents.hasNext());
        SplitSegmentRequest splitSegmentRequest = (SplitSegmentRequest) publishedEvents.next();
        assertEquals(expectedClientName, splitSegmentRequest.getClientName());
        assertEquals(expectedProcessorName, splitSegmentRequest.getProcessorName());
        assertEquals(expectedSegmentId, splitSegmentRequest.getSegmentId());
    }

    @Test
    public void testMergeSegmentConnectorCommandComingIn() {
        String expectedClientName = "clientName";
        String expectedProcessorName = "processorName";
        int expectedSegmentId = 1;
        ClientEventProcessorSegment testMergeMessage =
                ClientEventProcessorSegment.newBuilder()
                                           .setClient(expectedClientName)
                                           .setProcessorName(expectedProcessorName)
                                           .setSegmentIdentifier(expectedSegmentId)
                                           .build();

        FakeStreamObserver<ConnectorResponse> responseStream = new FakeStreamObserver<>();
        StreamObserver<ConnectorCommand> requestStream = testSubject.openStream(responseStream);

        requestStream.onNext(ConnectorCommand.newBuilder().setMergeSegment(testMergeMessage).build());

        assertEquals(0, responseStream.values().size());
        requestStream.onCompleted();

        Iterator<Object> publishedEvents = eventPublisher.events().iterator();
        assertTrue(publishedEvents.hasNext());
        MergeSegmentRequest mergeSegmentRequest = (MergeSegmentRequest) publishedEvents.next();
        assertEquals(expectedClientName, mergeSegmentRequest.getClientName());
        assertEquals(expectedProcessorName, mergeSegmentRequest.getProcessorName());
        assertEquals(expectedSegmentId, mergeSegmentRequest.getSegmentId());
    }

    @Test
    public void testAxonServerNodeDisconnected() {
        testSubject.on(new TopologyEvents.ApplicationConnected("context", "component", "client", "proxy"));
        assertFalse(testSubject.connectedClients().isEmpty());
        testSubject.on(new ClusterEvents.AxonServerInstanceDisconnected("proxy"));
        assertTrue(testSubject.connectedClients().isEmpty());
    }
}
