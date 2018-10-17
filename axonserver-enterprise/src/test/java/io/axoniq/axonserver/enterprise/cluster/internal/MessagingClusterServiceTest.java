package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.TopologyEvents;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.manager.EventStoreManager;
import io.axoniq.axonserver.enterprise.context.ContextController;
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
import io.axoniq.platform.application.ApplicationController;
import io.axoniq.platform.application.ApplicationModelController;
import io.axoniq.platform.user.UserController;
import io.grpc.stub.StreamObserver;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.runners.*;

import java.util.stream.Stream;

import static io.axoniq.axonserver.grpc.internal.ConnectorResponse.ResponseCase.CONNECT_RESPONSE;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Author: marc
 */
@RunWith(MockitoJUnitRunner.class)
public class MessagingClusterServiceTest {
    private MessagingClusterService messagingClusterService;
    @Mock
    private ApplicationController applicationController;
    @Mock
    private CommandDispatcher commandDispatcher;
    @Mock
    private QueryDispatcher queryDispatcher;
    @Mock
    private ClusterController clusterController;

    @Mock
    private UserController userController;

    private FakeApplicationEventPublisher eventPublisher;

    @Mock
    private ContextController contextController;

    @Mock
    private ApplicationModelController applicationModelController;

    @Mock
    private EventStoreManager eventStoreManager;

    @Before
    public void setUp() {
        this.eventPublisher = new FakeApplicationEventPublisher();
        messagingClusterService = new MessagingClusterService(
                commandDispatcher, queryDispatcher, clusterController, userController, applicationController,
                applicationModelController,
                contextController,eventStoreManager, eventPublisher);
        when(clusterController.nodes()).thenReturn(Stream.empty());
    }

    @Test
    public void connect() {
        CountingStreamObserver<ConnectorResponse> responseStream = new CountingStreamObserver<>();
        StreamObserver<ConnectorCommand> requestStream = messagingClusterService.openStream(responseStream);
        requestStream.onNext(ConnectorCommand.newBuilder().setConnect(NodeInfo.newBuilder().setNodeName("application-server1")).build());
        assertEquals(1, responseStream.count); // connect response
        assertEquals(CONNECT_RESPONSE, responseStream.responseList.get(0).getResponseCase());
    }

    @Test
    public void subscribeQuery() {
        CountingStreamObserver<ConnectorResponse> responseStream = new CountingStreamObserver<>();
        StreamObserver<ConnectorCommand> requestStream = messagingClusterService.openStream(responseStream);
        requestStream.onNext(ConnectorCommand.newBuilder().setSubscribeQuery(InternalQuerySubscription.newBuilder()
                .setQuery(
                        QuerySubscription.newBuilder()
                                .setQuery("query")
                                .setComponentName("Component")
                                .setClientId("Client")
                                .setResultName("Result")
                                .setNrOfHandlers(1))
                .setContext(Topology.DEFAULT_CONTEXT))
                .build());
        assertEquals(0, responseStream.count);

        requestStream.onCompleted();
        Object next = eventPublisher.events().iterator().next();
        assertTrue(next instanceof TopologyEvents.ApplicationConnected);
    }

    @Test
    public void queryFlowControl() {
        CountingStreamObserver<ConnectorResponse> responseStream = new CountingStreamObserver<>();
        StreamObserver<ConnectorCommand> requestStream = messagingClusterService.openStream(responseStream);
        requestStream.onNext(ConnectorCommand.newBuilder().setFlowControl(InternalFlowControl.newBuilder()
                .setGroup(Group.QUERY)
                .setNodeName("node1")
                .setPermits(1000)
        ).build());
        assertEquals(0, responseStream.count);
        requestStream.onCompleted();
    }

    @Test
    public void commandFlowControl() {
        CountingStreamObserver<ConnectorResponse> responseStream = new CountingStreamObserver<>();
        StreamObserver<ConnectorCommand> requestStream = messagingClusterService.openStream(responseStream);
        requestStream.onNext(ConnectorCommand.newBuilder().setFlowControl(InternalFlowControl.newBuilder()
                .setGroup(Group.COMMAND)
                .setNodeName("node1")
                .setPermits(1000)
        ).build());
        assertEquals(0, responseStream.count);
        requestStream.onCompleted();
    }


}