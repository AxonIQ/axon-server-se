package io.axoniq.axonserver.enterprise.taskscheduler.task;

import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.replication.admin.RaftConfigService;
import io.axoniq.axonserver.enterprise.replication.admin.RaftConfigServiceFactory;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupMember;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class AddNodeToReplicationGroupTaskTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    ClusterController clusterController;

    @Mock
    RaftConfigServiceFactory raftServiceFactory;

    @Mock
    private RaftConfigService raftConfigService;

    private AddNodeToReplicationGroupTask testSubject;

    @Before
    public void setUp() {
        testSubject = new AddNodeToReplicationGroupTask(clusterController, raftServiceFactory);
        when(raftServiceFactory.getRaftConfigService()).thenReturn(raftConfigService);

        when(clusterController.getMe().getInternalHostName()).thenReturn("internal-hostname");
        when(clusterController.getMe().getHostName()).thenReturn("hostname");
        when(clusterController.getMe().getGrpcInternalPort()).thenReturn(1111);
        when(clusterController.getName()).thenReturn("node-1");
    }

    @Test
    public void whenAddNodeToReplicationGroupThenFirstTryCreateReplication() {
        AddNodeToReplicationGroup addNodeToReplicationGroup = new AddNodeToReplicationGroup("rp", "PRIMARY");
        testSubject.executeAsync("", addNodeToReplicationGroup);

        ReplicationGroupMember build = ReplicationGroupMember
                .newBuilder()
                .setNodeName("node-1")
                .setHost("hostname")
                .setPort(1111)
                .setRole(Role.PRIMARY)
                .build();

        verify(raftConfigService).createReplicationGroup("rp", Collections.singletonList(build));
        verify(raftConfigService).addNodeToReplicationGroup("rp","node-1", Role.PRIMARY);
    }

    @Test
    public void whenReplicationGroupAlreadyExistsThenJustAddNode() {
        AddNodeToReplicationGroup addNodeToReplicationGroup = new AddNodeToReplicationGroup("rp", "PRIMARY");

        doThrow(new MessagingPlatformException(ErrorCode.CONTEXT_EXISTS,"")).when(raftConfigService).createReplicationGroup(anyString(),anyList());

        testSubject.executeAsync("", addNodeToReplicationGroup);

        verify(raftConfigService, times(1)).createReplicationGroup(any(),any());
        verify(raftConfigService).addNodeToReplicationGroup("rp","node-1", Role.PRIMARY);
    }

    @Test(expected = IllegalAccessError.class)
    public void whenThrowsUnknownErrorThenDoNotAddNode() {
        AddNodeToReplicationGroup addNodeToReplicationGroup = new AddNodeToReplicationGroup("rp", "PRIMARY");

        doThrow(new IllegalAccessError("")).when(raftConfigService).createReplicationGroup(anyString(),anyList());

        testSubject.executeAsync("", addNodeToReplicationGroup);

        verify(raftConfigService, never()).createReplicationGroup(any(),any());
        verify(raftConfigService, never()).addNodeToReplicationGroup(any(),any(),any());
    }

}