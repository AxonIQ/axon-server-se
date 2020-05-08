package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.TestSystemInfoProvider;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.config.ClusterConfiguration;
import io.axoniq.axonserver.grpc.internal.*;
import io.axoniq.axonserver.test.FakeStreamObserver;
import org.junit.Before;
import org.junit.Test;

import static io.axoniq.axonserver.grpc.internal.ConnectorCommand.RequestCase.*;
import static org.junit.Assert.assertEquals;

/**
 * @author Marc Gathier
 */
public class ClusterFlowControlStreamObserverTest {

    private ClusterFlowControlStreamObserver testSubject;
    private FakeStreamObserver<ConnectorCommand> delegate;
    private MessagingPlatformConfiguration messagingPlatformConfiguration;
    private ClusterConfiguration clusterConfiguration;

    @Before
    public void setup() {
        TestSystemInfoProvider environment = new TestSystemInfoProvider();
        messagingPlatformConfiguration = new MessagingPlatformConfiguration(environment);
        messagingPlatformConfiguration.setName("name");
        clusterConfiguration = new ClusterConfiguration();
        clusterConfiguration.getCommandFlowControl().setInitialPermits(1);
        clusterConfiguration.getCommandFlowControl().setNewPermits(5);
        clusterConfiguration.getCommandFlowControl().setThreshold(0);
        clusterConfiguration.getQueryFlowControl().setInitialPermits(1);
        clusterConfiguration.getQueryFlowControl().setNewPermits(1);
        clusterConfiguration.getQueryFlowControl().setThreshold(0);
        delegate = new FakeStreamObserver<>();
        testSubject = new ClusterFlowControlStreamObserver(delegate);
    }

    @Test
    public void onNextCommand() {
        testSubject.initCommandFlowControl(messagingPlatformConfiguration.getName(),
                                           clusterConfiguration.getCommandFlowControl());
        testSubject.onNext(ConnectorCommand.newBuilder()
                                           .setCommandResponse(ForwardedCommandResponse.newBuilder().build()).build());
        assertEquals(3, delegate.values().size());
        assertEquals(COMMAND_RESPONSE, delegate.values().get(1).getRequestCase());
        assertEquals(FLOW_CONTROL, delegate.values().get(2).getRequestCase());
        testSubject.onNext(ConnectorCommand.newBuilder()
                                           .setCommandResponse(ForwardedCommandResponse.newBuilder().build()).build());
        assertEquals(4, delegate.values().size());
        assertEquals(COMMAND_RESPONSE, delegate.values().get(3).getRequestCase());
    }
    @Test
    public void onNextQuery() {
        testSubject.initQueryFlowControl(messagingPlatformConfiguration.getName(),
                                         clusterConfiguration.getQueryFlowControl());
        testSubject.onNext(ConnectorCommand.newBuilder().setQueryResponse(ForwardedQueryResponse.getDefaultInstance())
                                           .build());
        assertEquals(3, delegate.values().size());
        assertEquals(QUERY_RESPONSE, delegate.values().get(1).getRequestCase());
        assertEquals(FLOW_CONTROL, delegate.values().get(2).getRequestCase());
        testSubject.onNext(ConnectorCommand.newBuilder().setQueryResponse(ForwardedQueryResponse.getDefaultInstance())
                                           .build());
        assertEquals(5, delegate.values().size());
        assertEquals(QUERY_RESPONSE, delegate.values().get(3).getRequestCase());
    }
    @Test
    public void onNextOther() {
        testSubject.initQueryFlowControl(messagingPlatformConfiguration.getName(), clusterConfiguration.getQueryFlowControl());
        testSubject.initCommandFlowControl(messagingPlatformConfiguration.getName(),
                                           clusterConfiguration.getCommandFlowControl());
        assertEquals(2, delegate.values().size());
        testSubject.onNext(ConnectorCommand.newBuilder().setDeleteNode(DeleteNode.newBuilder().build()).build());
        assertEquals(3, delegate.values().size());
        testSubject.onNext(ConnectorCommand.newBuilder().setDeleteNode(DeleteNode.newBuilder().build()).build());
        assertEquals(4, delegate.values().size());
    }

    @Test
    public void initCommandFlowControl() {
        testSubject.initCommandFlowControl(messagingPlatformConfiguration.getName(),
                                           clusterConfiguration.getCommandFlowControl());
        assertEquals(1, delegate.values().size());
        assertEquals(FLOW_CONTROL, delegate.values().get(0).getRequestCase());
        assertEquals(1, delegate.values().get(0).getFlowControl().getPermits());
        assertEquals(Group.COMMAND, delegate.values().get(0).getFlowControl().getGroup());
    }

    @Test
    public void initQueryFlowControl() {
        testSubject.initQueryFlowControl(messagingPlatformConfiguration.getName(),
                                         clusterConfiguration.getQueryFlowControl());
        assertEquals(1, delegate.values().size());
        assertEquals(FLOW_CONTROL, delegate.values().get(0).getRequestCase());
        assertEquals(1, delegate.values().get(0).getFlowControl().getPermits());
        assertEquals(Group.QUERY, delegate.values().get(0).getFlowControl().getGroup());
    }

}
