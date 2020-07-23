package io.axoniq.axonserver.enterprise.replication.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.replication.DefaultSnapshotContext;
import io.axoniq.axonserver.cluster.snapshot.SnapshotContext;
import io.axoniq.axonserver.enterprise.jpa.AdminReplicationGroup;
import io.axoniq.axonserver.enterprise.jpa.AdminReplicationGroupMember;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.replication.admin.AdminReplicationGroupController;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import io.axoniq.axonserver.grpc.internal.ContextConfiguration;
import org.junit.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class LegacyReplicationGroupSnapshotDataStoreTest {

    private LegacyReplicationGroupSnapshotDataStore testSubject;

    @Before
    public void setUp() {
        AdminReplicationGroupController adminReplicationGroupController = mock(AdminReplicationGroupController.class);
        List<AdminReplicationGroup> replicationGroups = new ArrayList<>();
        AdminReplicationGroup replicationGroup = new AdminReplicationGroup();
        replicationGroup.setName("demo");
        AdminReplicationGroupMember member = new AdminReplicationGroupMember();
        member.setClusterNode(new ClusterNode("node1", "node1", "node1", 1, 2, 3));
        member.setRole(Role.PRIMARY);
        member.setClusterNodeLabel("Label1");
        replicationGroup.addMember(member);
        replicationGroups.add(replicationGroup);
        when(adminReplicationGroupController.findAll()).thenReturn(replicationGroups);
        testSubject = new LegacyReplicationGroupSnapshotDataStore("_admin", adminReplicationGroupController);
    }

    @Test
    public void streamSnapshotData() {
        SnapshotContext installationContext = new DefaultSnapshotContext(Collections.emptyMap(),
                                                                         Collections.emptyMap(),
                                                                         true,
                                                                         Role.PRIMARY);
        List<SerializedObject> messages = testSubject.streamSnapshotData(installationContext).collectList().block();
        assertEquals(0, messages.size());
    }

    @Test
    public void streamSnapshotDataNoReplicationGroups() throws InvalidProtocolBufferException {
        SnapshotContext installationContext = new DefaultSnapshotContext(Collections.emptyMap(),
                                                                         Collections.emptyMap(),
                                                                         false,
                                                                         Role.PRIMARY);
        List<SerializedObject> messages = testSubject.streamSnapshotData(installationContext).collectList().block();
        assertEquals(1, messages.size());
        ContextConfiguration contextConfiguration = ContextConfiguration.parseFrom(messages.get(0).getData());
        assertEquals("demo", contextConfiguration.getContext());
        assertEquals(1, contextConfiguration.getNodesCount());
        assertEquals("node1", contextConfiguration.getNodes(0).getNode().getNodeName());
    }
}