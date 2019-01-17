package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.TestSystemInfoProvider;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.grpc.internal.ConnectorCommand;
import io.axoniq.axonserver.grpc.internal.DeleteNode;
import io.axoniq.axonserver.grpc.internal.ForwardedCommandResponse;
import io.axoniq.axonserver.grpc.internal.Group;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.axonserver.util.CountingStreamObserver;
import org.junit.*;

import static io.axoniq.axonserver.grpc.internal.ConnectorCommand.RequestCase.*;
import static org.junit.Assert.*;

/**
 * Author: marc
 */
public class ClusterFlowControlStreamObserverTest {
    private ClusterFlowControlStreamObserver testSubject;
    private CountingStreamObserver<ConnectorCommand> delegate;
    private MessagingPlatformConfiguration messagingPlatformConfiguration;
    @Before
    public void setup() {
        TestSystemInfoProvider environment = new TestSystemInfoProvider();
        messagingPlatformConfiguration = new MessagingPlatformConfiguration(environment);
        messagingPlatformConfiguration.setName("name");
        messagingPlatformConfiguration.getCommandFlowControl().setInitialPermits(1);
        messagingPlatformConfiguration.getCommandFlowControl().setNewPermits(5);
        messagingPlatformConfiguration.getCommandFlowControl().setThreshold(0);
        messagingPlatformConfiguration.getQueryFlowControl().setInitialPermits(1);
        messagingPlatformConfiguration.getQueryFlowControl().setNewPermits(1);
        messagingPlatformConfiguration.getQueryFlowControl().setThreshold(0);
        delegate = new CountingStreamObserver<>();
        testSubject = new ClusterFlowControlStreamObserver(delegate);
    }

    @Test
    public void onNextCommand() {
        testSubject.initCommandFlowControl(messagingPlatformConfiguration);
        testSubject.onNext(ConnectorCommand.newBuilder().setCommandResponse(ForwardedCommandResponse.newBuilder().build()).build());
        assertEquals(3, delegate.count);
        assertEquals( COMMAND_RESPONSE, delegate.responseList.get(1).getRequestCase());
        assertEquals( FLOW_CONTROL, delegate.responseList.get(2).getRequestCase());
        testSubject.onNext(ConnectorCommand.newBuilder().setCommandResponse(ForwardedCommandResponse.newBuilder().build()).build());
        assertEquals(4, delegate.count);
        assertEquals( COMMAND_RESPONSE, delegate.responseList.get(3).getRequestCase());
    }
    @Test
    public void onNextQuery() {
        testSubject.initQueryFlowControl(messagingPlatformConfiguration);
        testSubject.onNext(ConnectorCommand.newBuilder().setQueryResponse(QueryResponse.newBuilder().build()).build());
        assertEquals(3, delegate.count);
        assertEquals( QUERY_RESPONSE, delegate.responseList.get(1).getRequestCase());
        assertEquals( FLOW_CONTROL, delegate.responseList.get(2).getRequestCase());
        testSubject.onNext(ConnectorCommand.newBuilder().setQueryResponse(QueryResponse.newBuilder().build()).build());
        assertEquals(4, delegate.count);
        assertEquals( QUERY_RESPONSE, delegate.responseList.get(3).getRequestCase());
    }
    @Test
    public void onNextOther() {
        testSubject.initQueryFlowControl(messagingPlatformConfiguration);
        testSubject.initCommandFlowControl(messagingPlatformConfiguration);
        assertEquals(2, delegate.count);
        testSubject.onNext(ConnectorCommand.newBuilder().setDeleteNode(DeleteNode.newBuilder().build()).build());
        assertEquals(3, delegate.count);
        testSubject.onNext(ConnectorCommand.newBuilder().setDeleteNode(DeleteNode.newBuilder().build()).build());
        assertEquals(4, delegate.count);
    }

    @Test
    public void initCommandFlowControl() {
        testSubject.initCommandFlowControl(messagingPlatformConfiguration);
        assertEquals(1, delegate.count);
        assertEquals( FLOW_CONTROL, delegate.responseList.get(0).getRequestCase());
        assertEquals( 1, delegate.responseList.get(0).getFlowControl().getPermits());
        assertEquals( Group.COMMAND, delegate.responseList.get(0).getFlowControl().getGroup());
    }

    @Test
    public void initQueryFlowControl() {
        testSubject.initQueryFlowControl(messagingPlatformConfiguration);
        assertEquals(1, delegate.count);
        assertEquals( FLOW_CONTROL, delegate.responseList.get(0).getRequestCase());
        assertEquals( 1, delegate.responseList.get(0).getFlowControl().getPermits());
        assertEquals( Group.QUERY, delegate.responseList.get(0).getFlowControl().getGroup());
    }

}