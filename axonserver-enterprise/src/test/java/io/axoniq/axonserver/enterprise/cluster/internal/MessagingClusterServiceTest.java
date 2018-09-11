package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonhub.QuerySubscription;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.context.ContextController;
import io.axoniq.axonhub.internal.grpc.ConnectorCommand;
import io.axoniq.axonhub.internal.grpc.ConnectorResponse;
import io.axoniq.axonhub.internal.grpc.Group;
import io.axoniq.axonhub.internal.grpc.InternalFlowControl;
import io.axoniq.axonhub.internal.grpc.InternalQuerySubscription;
import io.axoniq.axonhub.internal.grpc.NodeInfo;
import io.axoniq.axonserver.message.command.CommandDispatcher;
import io.axoniq.axonserver.message.query.QueryDispatcher;
import io.axoniq.axonserver.spring.FakeApplicationEventPublisher;
import io.axoniq.axonserver.util.CountingStreamObserver;
import io.axoniq.platform.application.ApplicationController;
import io.axoniq.platform.user.UserController;
import io.grpc.stub.StreamObserver;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.runners.*;

import java.util.Collections;

import static io.axoniq.axonhub.internal.grpc.ConnectorResponse.ResponseCase.CONNECT_RESPONSE;
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

    @Before
    public void setUp() {
        this.eventPublisher = new FakeApplicationEventPublisher();
        messagingClusterService = new MessagingClusterService(
                                                              commandDispatcher, queryDispatcher, clusterController, userController, applicationController, contextController, eventPublisher);
        when(clusterController.getMyContexts()).thenReturn(Collections.emptySet());
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
                                .setClientName("Client")
                                .setResultName("Result")
                                .setNrOfHandlers(1))
                .setContext(ContextController.DEFAULT))
                .build());
        assertEquals(0, responseStream.count);

        requestStream.onCompleted();
        Object next = eventPublisher.events().iterator().next();
        assertTrue(next instanceof ClusterEvents.ApplicationConnected);
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