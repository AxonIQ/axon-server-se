package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.applicationevents.EventProcessorEvents.MergeSegmentRequest;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.SplitSegmentRequest;
import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.grpc.internal.ClientEventProcessorSegment;
import io.axoniq.axonserver.grpc.internal.ConnectRequest;
import io.axoniq.axonserver.grpc.internal.ConnectorCommand;
import io.axoniq.axonserver.grpc.internal.ConnectorResponse;
import io.axoniq.axonserver.grpc.internal.Group;
import io.axoniq.axonserver.grpc.internal.InternalFlowControl;
import io.axoniq.axonserver.grpc.internal.InternalQuerySubscription;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.grpc.query.QuerySubscription;
import io.axoniq.axonserver.message.command.CommandDispatcher;
import io.axoniq.axonserver.message.query.QueryDispatcher;
import io.axoniq.axonserver.spring.FakeApplicationEventPublisher;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.axonserver.util.CountingStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.*;

import java.util.Iterator;

import static io.axoniq.axonserver.grpc.internal.ConnectorResponse.ResponseCase.CONNECT_RESPONSE;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class MessagingClusterServiceTest {

    private final CommandDispatcher commandDispatcher = mock(CommandDispatcher.class);
    private final QueryDispatcher queryDispatcher = mock(QueryDispatcher.class);
    private final ClusterController clusterController = mock(ClusterController.class);
    private final FakeApplicationEventPublisher eventPublisher = new FakeApplicationEventPublisher();

    private MessagingClusterService testSubject;

    @Before
    public void setUp() {
        testSubject = new MessagingClusterService(
                commandDispatcher, queryDispatcher, clusterController, eventPublisher
        );
    }

    @Test
    public void connect() {
        CountingStreamObserver<ConnectorResponse> responseStream = new CountingStreamObserver<>();
        StreamObserver<ConnectorCommand> requestStream = testSubject.openStream(responseStream);
        requestStream.onNext(ConnectorCommand.newBuilder().setConnect(
                ConnectRequest.newBuilder().setNodeInfo(NodeInfo.newBuilder().setNodeName("application-server1"))
        ).build());
        assertEquals(1, responseStream.count); // connect response
        assertEquals(CONNECT_RESPONSE, responseStream.responseList.get(0).getResponseCase());
    }

    @Test
    public void subscribeQuery() {
        CountingStreamObserver<ConnectorResponse> responseStream = new CountingStreamObserver<>();

        InternalQuerySubscription testMessage =
                InternalQuerySubscription.newBuilder()
                                         .setQuery(QuerySubscription.newBuilder()
                                                                    .setQuery("query")
                                                                    .setComponentName("Component")
                                                                    .setClientId("Client")
                                                                    .setResultName("Result")
                                                                    .setNrOfHandlers(1))
                                         .setContext(Topology.DEFAULT_CONTEXT)
                                         .build();

        StreamObserver<ConnectorCommand> requestStream = testSubject.openStream(responseStream);
        requestStream.onNext(ConnectorCommand.newBuilder().setSubscribeQuery(testMessage).build());
        assertEquals(0, responseStream.count);

        requestStream.onCompleted();
        Object next = eventPublisher.events().iterator().next();
        assertTrue(next instanceof TopologyEvents.ApplicationConnected);
    }

    @Test
    public void queryFlowControl() {
        InternalFlowControl testMessage = InternalFlowControl.newBuilder()
                                                             .setGroup(Group.QUERY)
                                                             .setNodeName("node1")
                                                             .setPermits(1000)
                                                             .build();
        CountingStreamObserver<ConnectorResponse> responseStream = new CountingStreamObserver<>();
        StreamObserver<ConnectorCommand> requestStream = testSubject.openStream(responseStream);

        requestStream.onNext(ConnectorCommand.newBuilder().setFlowControl(testMessage).build());

        assertEquals(0, responseStream.count);
        requestStream.onCompleted();
    }

    @Test
    public void commandFlowControl() {
        InternalFlowControl testMessage = InternalFlowControl.newBuilder()
                                                             .setGroup(Group.COMMAND)
                                                             .setNodeName("node1")
                                                             .setPermits(1000)
                                                             .build();

        CountingStreamObserver<ConnectorResponse> responseStream = new CountingStreamObserver<>();
        StreamObserver<ConnectorCommand> requestStream = testSubject.openStream(responseStream);

        requestStream.onNext(ConnectorCommand.newBuilder().setFlowControl(testMessage).build());

        assertEquals(0, responseStream.count);
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

        CountingStreamObserver<ConnectorResponse> responseStream = new CountingStreamObserver<>();
        StreamObserver<ConnectorCommand> requestStream = testSubject.openStream(responseStream);

        requestStream.onNext(ConnectorCommand.newBuilder().setSplitSegment(testSplitMessage).build());

        assertEquals(0, responseStream.count);
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

        CountingStreamObserver<ConnectorResponse> responseStream = new CountingStreamObserver<>();
        StreamObserver<ConnectorCommand> requestStream = testSubject.openStream(responseStream);

        requestStream.onNext(ConnectorCommand.newBuilder().setMergeSegment(testMergeMessage).build());

        assertEquals(0, responseStream.count);
        requestStream.onCompleted();

        Iterator<Object> publishedEvents = eventPublisher.events().iterator();
        assertTrue(publishedEvents.hasNext());
        MergeSegmentRequest mergeSegmentRequest = (MergeSegmentRequest) publishedEvents.next();
        assertEquals(expectedClientName, mergeSegmentRequest.getClientName());
        assertEquals(expectedProcessorName, mergeSegmentRequest.getProcessorName());
        assertEquals(expectedSegmentId, mergeSegmentRequest.getSegmentId());
    }
}
