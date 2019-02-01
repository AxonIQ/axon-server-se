package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.access.application.ApplicationController;
import io.axoniq.axonserver.access.modelversion.ModelVersionController;
import io.axoniq.axonserver.access.user.UserController;
import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.GrpcRaftController;
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
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.junit.*;

import static io.axoniq.axonserver.grpc.internal.ConnectorResponse.ResponseCase.CONNECT_RESPONSE;
import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
@RunWith(MockitoJUnitRunner.class)
public class MessagingClusterServiceTest {
    private MessagingClusterService messagingClusterService;
    @Mock
    private CommandDispatcher commandDispatcher;
    @Mock
    private QueryDispatcher queryDispatcher;
    @Mock
    private ClusterController clusterController;

    private FakeApplicationEventPublisher eventPublisher;

    @Mock
    private GrpcRaftController grpcRaftController;

    @Before
    public void setUp() {
        this.eventPublisher = new FakeApplicationEventPublisher();
        messagingClusterService = new MessagingClusterService(
                commandDispatcher, queryDispatcher, clusterController, grpcRaftController, eventPublisher);
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
