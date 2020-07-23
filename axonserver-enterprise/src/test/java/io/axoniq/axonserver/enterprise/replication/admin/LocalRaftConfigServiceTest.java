package io.axoniq.axonserver.enterprise.replication.admin;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.access.application.AdminApplication;
import io.axoniq.axonserver.access.application.AdminApplicationContext;
import io.axoniq.axonserver.access.application.AdminApplicationContextRole;
import io.axoniq.axonserver.access.application.AdminApplicationController;
import io.axoniq.axonserver.access.jpa.User;
import io.axoniq.axonserver.access.jpa.UserRole;
import io.axoniq.axonserver.access.user.UserController;
import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.config.FeatureChecker;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.config.SystemInfoProvider;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.context.AdminContextController;
import io.axoniq.axonserver.enterprise.jpa.AdminContext;
import io.axoniq.axonserver.enterprise.jpa.AdminReplicationGroup;
import io.axoniq.axonserver.enterprise.jpa.AdminReplicationGroupMember;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.replication.GrpcRaftController;
import io.axoniq.axonserver.enterprise.replication.group.RaftGroupServiceFactory;
import io.axoniq.axonserver.enterprise.replication.logconsumer.AdminContextConsumer;
import io.axoniq.axonserver.enterprise.taskscheduler.TaskPublisher;
import io.axoniq.axonserver.enterprise.taskscheduler.task.PrepareDeleteNodeFromContextTask;
import io.axoniq.axonserver.enterprise.taskscheduler.task.UnregisterNodeTask;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import io.axoniq.axonserver.grpc.internal.Application;
import io.axoniq.axonserver.grpc.internal.ApplicationContextRole;
import io.axoniq.axonserver.grpc.internal.ContextRole;
import io.axoniq.axonserver.grpc.internal.DeleteContextRequest;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;
import io.axoniq.axonserver.grpc.internal.ReplicationGroup;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupConfiguration;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupContext;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupMember;
import io.axoniq.axonserver.grpc.internal.UserContextRole;
import io.axoniq.axonserver.licensing.LicenseManager;
import io.axoniq.axonserver.taskscheduler.TransientException;
import org.jetbrains.annotations.NotNull;
import org.junit.*;
import org.springframework.context.ApplicationEventPublisher;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class LocalRaftConfigServiceTest {

    private LocalRaftConfigService testSubject;
    private FakeRaftGroupService fakeRaftGroupService = new FakeRaftGroupService();
    private TaskPublisher taskPublisher;
    private Map<String, Set<String>> scheduledTasks = new HashMap<>();
    private Map<String, AdminReplicationGroup> replicationGroups = new HashMap<>();
    private Map<String, AdminContext> contexts = new HashMap<>();
    private Map<String, ClusterNode> nodes = new HashMap<>();
    private Map<String, AdminApplication> applications = new HashMap<>();
    private Map<String, User> users = new HashMap<>();
    private RaftNode adminNode = mock(RaftNode.class);
    private List<SerializedObject> adminEntries = new ArrayList<>();
    private GrpcRaftController grpcRaftController;
    private RaftGroupServiceFactory raftGroupServiceFactory = mock(RaftGroupServiceFactory.class);

    @Before
    public void setUp() {
        when(adminNode.appendEntry(anyString(), any())).then(invocation -> {
            adminEntries.add(SerializedObject.newBuilder()
                                             .setType(invocation.getArgument(0))
                                             .setData(ByteString.copyFrom((byte[]) invocation.getArgument(1)))
                                             .build());
            return CompletableFuture.completedFuture(null);
        });

        RaftGroup adminRaftGroup = mock(RaftGroup.class);
        grpcRaftController = mock(GrpcRaftController.class);
        when(grpcRaftController.initRaftGroup(anyString(), anyString(), anyString())).then(invocation -> {
            String replicationGroup = (String) invocation.getArgument(0);
            String node = (String) invocation.getArgument(2);
            replicationGroups.put(replicationGroup, givenReplicationGroup(replicationGroup, node));
            return adminRaftGroup;
        });
        when(grpcRaftController.waitForLeader(any())).thenReturn(adminNode);
        when(grpcRaftController.getRaftNode("_admin")).thenReturn(adminNode);

        AdminContextController contextcontroller = mock(AdminContextController.class);
        when(contextcontroller.getContexts()).then(invocation -> contexts.values().stream());
        when(contextcontroller.getContext(anyString())).then(invocation -> contexts.get(invocation.getArgument(0)));
        ClusterController clusterController = mock(ClusterController.class);
        when(clusterController.getNode(anyString())).then(invocation -> nodes.get(invocation.getArgument(0)));
        when(clusterController.getRemoteConnections()).thenReturn(Collections.emptyList());
        when(clusterController.remoteNodeNames()).then(i -> nodes.keySet());


        when(raftGroupServiceFactory.getRaftGroupService(anyString())).thenReturn(fakeRaftGroupService);
        when(raftGroupServiceFactory.getRaftGroupServiceForNode(anyString())).thenReturn(fakeRaftGroupService);
        when(raftGroupServiceFactory.getRaftGroupServiceForNode(any(ClusterNode.class)))
                .thenReturn(fakeRaftGroupService);

        AdminApplicationController applicationController = mock(AdminApplicationController.class);
        when(applicationController.getApplications()).thenAnswer(i -> new ArrayList<>(applications.values()));
        when(applicationController.get(anyString())).thenAnswer(i -> applications.get(i.getArgument(0)));
        when(applicationController.hash(anyString())).thenReturn("hashhash");

        MessagingPlatformConfiguration messagingPlatformConfiguration = new MessagingPlatformConfiguration(new SystemInfoProvider() {
            @Override
            public int getPort() {
                return 0;
            }

            @Override
            public String getHostName() {
                return "localhost";
            }
        });

        when(grpcRaftController.waitForLeader(any())).thenReturn(adminNode);
        when(adminNode.addNode(any())).thenReturn(CompletableFuture.completedFuture(null));
        UserController userController = mock(UserController.class);
        when(userController.getUsers()).then(i -> new ArrayList<>(users.values()));
        when(userController.findUser(anyString())).then(i -> users.get(i.getArgument(0)));
        taskPublisher = new TaskPublisher(null, null, null) {
            @Override
            public CompletableFuture<String> publishScheduledTask(String context, String taskHandler, Object payload,
                                                                  Duration delay) {
                scheduledTasks.computeIfAbsent(context, c -> new CopyOnWriteArraySet<>()).add(taskHandler);
                return CompletableFuture.completedFuture(null);
            }
        };
        AdminReplicationGroupController adminReplicationGroupRepository = mock(AdminReplicationGroupController.class);
        when(adminReplicationGroupRepository.findByName(anyString())).then(invocation -> Optional
                .ofNullable(replicationGroups.get(invocation.getArgument(0))));
        when(adminReplicationGroupRepository.findAll()).then(invocation -> new ArrayList<>(replicationGroups.values()));
        LicenseManager licenseManager = mock(LicenseManager.class);

        FeatureChecker limits = mock(FeatureChecker.class);
        when(limits.getMaxClusterSize()).thenReturn(100);
        when(limits.getMaxContexts()).thenReturn(100);
        when(limits.isEnterprise()).thenReturn(true);
        when(licenseManager.readLicense()).thenReturn("SAMPLELICENSE".getBytes());
        testSubject = new LocalRaftConfigService(grpcRaftController,
                                                 contextcontroller,
                                                 clusterController,
                                                 raftGroupServiceFactory,
                                                 applicationController,
                                                 userController,
                                                 messagingPlatformConfiguration,
                                                 taskPublisher,
                                                 licenseManager,
                                                 limits,
                                                 adminReplicationGroupRepository,
                                                 mock(ApplicationEventPublisher.class));
    }

    @Test
    public void addNodeToReplicationGroup() throws InvalidProtocolBufferException {
        // given a replication group sample
        givenReplicationGroup("sample", "node1");
        // and a node node2
        givenNode("node2");
        // when I add an existing node to the replication group
        testSubject.addNodeToReplicationGroup("sample", "node2", Role.PRIMARY);
        // service sends message to admin node to mark the replication group as changes pending
        assertEquals(2, fakeRaftGroupService.logEntries("_admin").size());
        assertEquals("io.axoniq.axonserver.grpc.internal.ReplicationGroupConfiguration",
                     fakeRaftGroupService.logEntries("_admin").get(0).getType());
        ReplicationGroupConfiguration replicationGroupConfiguration = ReplicationGroupConfiguration.parseFrom(
                fakeRaftGroupService.logEntries("_admin").get(0).getData());
        assertTrue(replicationGroupConfiguration.getPending());
        assertEquals(1, replicationGroupConfiguration.getNodesCount());
        // service sends a message to replication group leader to add the node
        ReplicationGroupMember node = fakeRaftGroupService.groupNode("sample", "node2");
        assertNotNull(node);
        assertEquals(Role.PRIMARY, node.getRole());
        // service sends message to admin leader to update the configuration
        assertEquals("io.axoniq.axonserver.grpc.internal.ReplicationGroupConfiguration",
                     fakeRaftGroupService.logEntries("_admin").get(1).getType());
        replicationGroupConfiguration = ReplicationGroupConfiguration.parseFrom(fakeRaftGroupService
                                                                                        .logEntries("_admin").get(1)
                                                                                        .getData());
        assertFalse(replicationGroupConfiguration.getPending());
        assertEquals(2, replicationGroupConfiguration.getNodesCount());
    }

    @Test
    public void addNodeToReplicationGroupNoLeader() throws InvalidProtocolBufferException {
        // given a replication group sample
        givenReplicationGroup("sample", "node1");
        // and a node node2
        givenNode("node2");
        when(raftGroupServiceFactory.getRaftGroupService("sample"))
                .thenThrow(new MessagingPlatformException(ErrorCode.NO_LEADER_AVAILABLE,
                                                          "No leader"));

        // when I add a node to the replication group while there is no leader for the group
        try {
            testSubject.addNodeToReplicationGroup("sample", "node2", Role.PRIMARY);
            fail("Cannot add a node to a replication group without a leader for that group");
        } catch (MessagingPlatformException mpe) {
            assertEquals(2, fakeRaftGroupService.logEntries("_admin").size());
            ReplicationGroupConfiguration replicationGroupConfiguration = ReplicationGroupConfiguration.parseFrom(
                    fakeRaftGroupService.logEntries("_admin").get(0).getData());
            assertTrue(replicationGroupConfiguration.getPending());
            assertEquals(1, replicationGroupConfiguration.getNodesCount());
            replicationGroupConfiguration = ReplicationGroupConfiguration.parseFrom(fakeRaftGroupService.logEntries(
                    "_admin").get(1).getData());
            assertFalse(replicationGroupConfiguration.getPending());
            assertEquals(1, replicationGroupConfiguration.getNodesCount());
        }
    }

    @Test
    public void addNodeWithOtherRoleToReplicationGroup() throws InvalidProtocolBufferException {
        // given a replication group sample
        givenReplicationGroup("sample", "node1");
        // and a node node2
        givenNode("node2");
        // when I add an existing node to the replication group with role ACTIVE_BACKUP
        testSubject.addNodeToReplicationGroup("sample", "node2", Role.ACTIVE_BACKUP);
        // I expect that raft group received correct role
        ReplicationGroupMember node = fakeRaftGroupService.groupNode("sample", "node2");
        assertNotNull(node);
        assertEquals(Role.ACTIVE_BACKUP, node.getRole());
        // And the last admin log entry contains the configuration with the correct role
        assertEquals(2, fakeRaftGroupService.logEntries("_admin").size());
        ReplicationGroupConfiguration replicationGroupConfiguration = ReplicationGroupConfiguration.parseFrom(
                fakeRaftGroupService.logEntries("_admin").get(1).getData());
        assertTrue(replicationGroupConfiguration.getNodesList()
                                                .stream()
                                                .anyMatch(nodeInfoWithLabel -> nodeInfoWithLabel.getNode().getNodeName()
                                                                                                .equals("node2")
                                                        && nodeInfoWithLabel.getRole().equals(Role.ACTIVE_BACKUP)));
    }

    @Test
    public void addNodeToNonExistingReplicationGroup() {
        try {
            testSubject.addNodeToReplicationGroup("myContext", "node1", Role.PRIMARY);
            fail("Expect exception");
        } catch (MessagingPlatformException mpe) {
            assertEquals(ErrorCode.CONTEXT_NOT_FOUND, mpe.getErrorCode());
        }
    }

    @Test
    public void addNonExistingNodeToReplicationGroup() {
        try {
            givenReplicationGroup("sample", "node1");

            testSubject.addNodeToReplicationGroup("sample", "node3", Role.PRIMARY);

            fail("Expect exception");
        } catch (MessagingPlatformException mpe) {
            assertEquals(ErrorCode.NO_SUCH_NODE, mpe.getErrorCode());
        }
    }

    @Test
    public void addExistingMemberToReplicationGroup() {
        givenReplicationGroup("sample", "node1");

        testSubject.addNodeToReplicationGroup("sample", "node1", Role.PRIMARY);

        assertEquals(0, fakeRaftGroupService.logEntries("_admin").size());
        assertEquals(0, fakeRaftGroupService.logEntries("sample").size());
    }


    @Test
    public void addContext() {
        givenReplicationGroup("sample", "node1");
        testSubject.addContext("sample", "demo", Collections.singletonMap("metadata1", "value1"));

        assertEquals(1, fakeRaftGroupService.logEntries("_admin").size());
        assertEquals(1, fakeRaftGroupService.logEntries("sample").size());
        SerializedObject logEntry = fakeRaftGroupService.logEntries("sample").get(0);
        assertEquals(ReplicationGroupContext.class.getName(), logEntry.getType());

        logEntry = fakeRaftGroupService.logEntries("_admin").get(0);
        assertEquals("ADD_CONTEXT", logEntry.getType());
    }

    @Test
    public void addContextNoLeader() {
        givenReplicationGroup("sample", "node1");
        when(raftGroupServiceFactory.getRaftGroupService("sample"))
                .thenThrow(new MessagingPlatformException(ErrorCode.NO_LEADER_AVAILABLE,
                                                          "No leader for sample"));
        try {
            testSubject.addContext("sample", "demo", Collections.singletonMap("metadata1", "value1"));
            fail("should not be able to add a context when there is no leader for the replication group");
        } catch (MessagingPlatformException mpe) {
            assertEquals(0, fakeRaftGroupService.logEntries("_admin").size());
            assertEquals(0, fakeRaftGroupService.logEntries("sample").size());
        }
    }

    @Test
    public void addContextToAdminGroup() {
        givenReplicationGroup("_admin", "node1");
        try {
            testSubject.addContext("_admin", "demo", Collections.singletonMap("metadata1", "value1"));
            fail("should not be able to add a context to the _admin replication group");
        } catch (MessagingPlatformException mpe) {
            assertEquals(0, fakeRaftGroupService.logEntries("_admin").size());
            assertEquals(0, fakeRaftGroupService.logEntries("sample").size());
        }
    }

    @Test
    public void addContextWithUserAnApp() {
        givenReplicationGroup("sample", "node1");
        givenUser("demo", "*", "READ");
        givenApplication("demo", "*", "WRITE");
        testSubject.addContext("sample", "demo", Collections.singletonMap("metadata1", "value1"));

        assertEquals(1, fakeRaftGroupService.logEntries("_admin").size());
        assertEquals(3, fakeRaftGroupService.logEntries("sample").size());
        SerializedObject logEntry = fakeRaftGroupService.logEntries("sample").get(0);
        assertEquals(ReplicationGroupContext.class.getName(), logEntry.getType());
    }

    @Test
    public void deleteContext() {
        AdminReplicationGroup replicationGroup = givenReplicationGroup("sample", "node1");
        contexts.put("demo", replicationGroup.addContext("demo"));
        testSubject.deleteContext("demo", true);
        assertEquals(1, fakeRaftGroupService.logEntries("_admin").size());
        assertEquals("ADMIN_DELETE_CONTEXT", fakeRaftGroupService.logEntries("_admin").get(0).getType());
        assertEquals(1, fakeRaftGroupService.logEntries("sample").size());
        assertEquals(DeleteContextRequest.class.getName(), fakeRaftGroupService.logEntries("sample").get(0).getType());
    }

    @Test
    public void deleteAdminContext() {
        try {
            testSubject.deleteContext("_admin", true);
            fail("Expect exception");
        } catch(MessagingPlatformException mpe) {
            assertEquals(ErrorCode.CANNOT_DELETE_INTERNAL_CONTEXT, mpe.getErrorCode());
        }
    }

    @Test
    public void deleteNonExistingContext() {
        testSubject.deleteContext("demo", true);
    }

    @Test
    public void deleteNodeFromReplicationGroup() {
        givenReplicationGroup("sample", "node1", "node2");
        testSubject.deleteNodeFromReplicationGroup("sample", "node2");
        assertEquals(2, fakeRaftGroupService.logEntries("_admin").size());
        assertNull(fakeRaftGroupService.groupNode("sample", "node2"));
    }

    @Test
    public void deleteLeaderNodeFromReplicationGroup() {
        givenReplicationGroup("sample", "node1", "node2");
        fakeRaftGroupService.setLeader("sample", "node2");
        when(raftGroupServiceFactory.getLeader(anyString())).then(invocation -> fakeRaftGroupService
                .getLeader(invocation.getArgument(0)));
        testSubject.deleteNodeFromReplicationGroup("sample", "node2");
        assertEquals(2, fakeRaftGroupService.logEntries("_admin").size());
        assertNull(fakeRaftGroupService.groupNode("sample", "node2"));
    }

    @Test
    public void deleteLastNodeFromAdminReplicationGroup() {
        givenReplicationGroup("_admin", "node2");
        try {
            testSubject.deleteNodeFromReplicationGroup("_admin", "node2");
            fail("Expect exception");
        } catch (MessagingPlatformException mpe) {
            assertEquals(ErrorCode.CANNOT_REMOVE_LAST_NODE, mpe.getErrorCode());
        }
    }

    @Test
    public void deleteNode() {
        givenReplicationGroup("sample", "node1", "node2");
        testSubject.deleteNode("node2");
        assertEquals(1, scheduledTasks.size());
        Set<String> adminTasks = scheduledTasks.getOrDefault("_admin", Collections.emptySet());
        assertTrue(adminTasks.contains(PrepareDeleteNodeFromContextTask.class.getName()));
        assertTrue(adminTasks.contains(UnregisterNodeTask.class.getName()));
    }

    @Test
    public void deleteNonExistingNode() {
        givenReplicationGroup("sample", "node1", "node2");
        testSubject.deleteNode("node3");
        assertEquals(0, scheduledTasks.size());
    }

    @Test
    public void deleteNodeIfEmpty() {
        givenNode("node3");
        testSubject.deleteNodeIfEmpty("node3");
        assertEquals(1, fakeRaftGroupService.logEntries("_admin").size());
    }

    @Test
    public void deleteNodeIfEmptyTransientExceptionIfNotEmpty() {
        givenReplicationGroup("sample", "node2", "node3");
        try {
            testSubject.deleteNodeIfEmpty("node3");
            fail("was able to delete node with a replication group in other nodes");
        } catch (TransientException ex) {
            // expected
        }
    }

    @Test
    public void createReplicationGroup() {
        givenNode("node1");
        givenNode("node2");
        ReplicationGroup context = createReplicationGroupGrpc("second", Arrays.asList("node1", "node2"));
        testSubject.createReplicationGroup(context.getName(), context.getMembersList());
    }

    @Test(expected = Throwable.class)
    public void createExistingReplicationGroup() {
        givenReplicationGroup("twice", "node1");
        ReplicationGroup context = createReplicationGroupGrpc("twice", Arrays.asList("node1", "node2"));
        testSubject.createReplicationGroup(context.getName(), context.getMembersList());
    }

    @Test
    public void deleteReplicationGroup() {
        givenReplicationGroup("sample", "node1", "node2");
        testSubject.deleteReplicationGroup("sample", false);
        assertNull(fakeRaftGroupService.groupNode("sample", "node1"));
        assertNull(fakeRaftGroupService.groupNode("sample", "node2"));
        assertEquals(2, fakeRaftGroupService.logEntries("_admin").size());
        verify(raftGroupServiceFactory, times(0)).getRaftGroupServiceForNode("localhost");
        verify(raftGroupServiceFactory).getRaftGroupServiceForNode("node1");
        verify(raftGroupServiceFactory).getRaftGroupServiceForNode("node2");
    }

    @Test
    public void deleteReplicationGroupNotInAdmin() {
        givenNode("node1");
        givenNode("node2");
        fakeRaftGroupService.addReplicationGroup("sample", "node1");
        testSubject.deleteReplicationGroup("sample", false);
        assertNull(fakeRaftGroupService.groupNode("sample", "node1"));
        verify(raftGroupServiceFactory).getRaftGroupServiceForNode("localhost");
        verify(raftGroupServiceFactory).getRaftGroupServiceForNode("node1");
        verify(raftGroupServiceFactory).getRaftGroupServiceForNode("node2");
        assertEquals(0, fakeRaftGroupService.logEntries("_admin").size());
    }

    @Test
    public void join() {
        givenReplicationGroup("_admin", "node1", "node2");
        givenReplicationGroup("sample", "node1", "node2");

        when(adminNode.isLeader()).thenReturn(true);
        testSubject.join(NodeInfo.newBuilder().setNodeName("node3").setInternalHostName("node3").setHostName("node3")
                                 .addContexts(ContextRole.newBuilder().setName("sample").setNodeLabel("sample/node3"))
                                 .build());
        assertEquals(3, fakeRaftGroupService.logEntries("_admin").size());
        assertNull(fakeRaftGroupService.groupNode("_admin", "node3"));
        assertNotNull(fakeRaftGroupService.groupNode("sample", "node3"));
    }

    @Test
    public void joinNewContext() {
        givenReplicationGroup("_admin", "node1", "node2");
        when(adminNode.isLeader()).thenReturn(true);
        testSubject.join(NodeInfo.newBuilder().setNodeName("node3").setInternalHostName("node3").setHostName("node3")
                                 .addContexts(ContextRole.newBuilder().setName("sample2").setNodeLabel("sample2/node3"))
                                 .build());

        assertEquals(4, fakeRaftGroupService.logEntries("_admin").size());
        assertNull(fakeRaftGroupService.groupNode("_admin", "node3"));
        assertNull(fakeRaftGroupService.groupNode("sample2", "node1"));
        assertNull(fakeRaftGroupService.groupNode("sample2", "node2"));
        assertNotNull(fakeRaftGroupService.groupNode("sample2", "node3"));
    }

    @Test
    public void joinAllContexts() {
        givenReplicationGroup("_admin", "node1", "node2");
        givenReplicationGroup("sample", "node1", "node2");

        when(adminNode.isLeader()).thenReturn(true);
        testSubject.join(NodeInfo.newBuilder().setNodeName("node3").setInternalHostName("node3").setHostName("node3")
                                 .build());
        assertEquals(5, fakeRaftGroupService.logEntries("_admin").size());
        assertNotNull(fakeRaftGroupService.groupNode("_admin", "node3"));
        assertNotNull(fakeRaftGroupService.groupNode("sample", "node3"));
    }

    @Test
    public void joinNoContexts() {
        when(adminNode.isLeader()).thenReturn(true);
        testSubject.join(NodeInfo.newBuilder().setNodeName("node3").setInternalHostName("node3").setHostName("node3")
                                 .addContexts(ContextRole.newBuilder().setName("_none")).build());

        assertEquals(1, fakeRaftGroupService.logEntries("_admin").size());
    }

    @Test
    public void init() {
        // Given no prior configuration
        // When
        testSubject.init(Arrays.asList("default"));
        // Then
        verify(grpcRaftController, times(2)).initRaftGroup(anyString(), anyString(), anyString());

        // admin entries (replication group configuration, add context to admin, add context,
        //                default group configuration, add context to default)
        assertEquals(2, adminEntries.size());
        assertEquals(3, fakeRaftGroupService.logEntries("_admin").size());
        assertEquals(ReplicationGroupConfiguration.class.getName(), adminEntries.get(0).getType());
        assertEquals(io.axoniq.axonserver.grpc.internal.ReplicationGroupContext.class.getName(),
                     fakeRaftGroupService.logEntries("_admin").get(0).getType());
        assertEquals(AdminContextConsumer.ENTRY_TYPE, fakeRaftGroupService.logEntries("_admin").get(1).getType());
        assertEquals(ReplicationGroupConfiguration.class.getName(), adminEntries.get(1).getType());
        assertEquals(AdminContextConsumer.ENTRY_TYPE, fakeRaftGroupService.logEntries("_admin").get(2).getType());

        // default entries (add context)
        assertEquals(1, fakeRaftGroupService.logEntries("default").size());
        assertEquals(io.axoniq.axonserver.grpc.internal.ReplicationGroupContext.class.getName(),
                     fakeRaftGroupService.logEntries("default").get(0).getType());
    }

    @Test
    public void refreshTokenWithGivenToken() {
        givenApplication("MyApplication", "*", "USE_CONTEXT");
        Application application = Application.newBuilder().setName("MyApplication").setToken("a brand new token")
                                             .build();
        Application result = testSubject.refreshToken(application);
        assertEquals("a brand new token", result.getToken());
    }

    @Test
    public void refreshToken() {
        AdminReplicationGroup replicationGroup = givenReplicationGroup("sample", "node1");
        contexts.put("demo", replicationGroup.addContext("demo"));
        givenApplication("MyApplication", "*", "USE_CONTEXT");
        Application application = Application.newBuilder().setName("MyApplication").build();
        Application result = testSubject.refreshToken(application);
        assertFalse(result.getToken().isEmpty());
        assertEquals(1, fakeRaftGroupService.logEntries("sample").size());
        assertEquals(1, fakeRaftGroupService.logEntries("_admin").size());
    }

    @Test
    public void updateApplication() {
        AdminReplicationGroup replicationGroup = givenReplicationGroup("sample", "node1");
        contexts.put("demo", replicationGroup.addContext("demo"));
        Application application = Application.newBuilder()
                                             .setName("MyApplication")
                                             .addRolesPerContext(ApplicationContextRole.newBuilder()
                                                                                       .setContext("demo")
                                                                                       .addRoles("USE_CONTEXT")
                                                                                       .build())
                                             .build();

        Application result = testSubject.updateApplication(application);
        assertFalse(result.getToken().isEmpty());
        assertEquals(1, fakeRaftGroupService.logEntries("sample").size());
    }

    @Test
    public void updateApplicationAllContexts() {
        AdminReplicationGroup replicationGroup = givenReplicationGroup("sample", "node1");
        contexts.put("demo", replicationGroup.addContext("demo"));
        contexts.put("demo2", replicationGroup.addContext("demo2"));
        Application application = Application.newBuilder()
                                             .setName("MyApplication")
                                             .addRolesPerContext(ApplicationContextRole.newBuilder()
                                                                                       .setContext("*")
                                                                                       .addRoles("USE_CONTEXT")
                                                                                       .build())
                                             .build();

        Application result = testSubject.updateApplication(application);
        assertFalse(result.getToken().isEmpty());
        assertEquals(2, fakeRaftGroupService.logEntries("sample").size());
    }

    @Test
    public void updateUser() {
        AdminReplicationGroup adminGroup = givenReplicationGroup("_admin", "node1");
        contexts.put("_admin", adminGroup.addContext("_admin"));
        AdminReplicationGroup replicationGroup = givenReplicationGroup("sample", "node1");
        contexts.put("demo", replicationGroup.addContext("demo"));
        contexts.put("demo2", replicationGroup.addContext("demo2"));
        io.axoniq.axonserver.grpc.internal.User user = io.axoniq.axonserver.grpc.internal.User.newBuilder()
                                                                                              .setName("Name")
                                                                                              .addRoles(UserContextRole
                                                                                                                .newBuilder()
                                                                                                                .setContext(
                                                                                                                        "*")
                                                                                                                .setRole(
                                                                                                                        "USE_CONTEXT")
                                                                                                                .build())
                                                                                              .build();
        testSubject.updateUser(user);
        assertEquals(2, fakeRaftGroupService.logEntries("sample").size());
        assertEquals(2, fakeRaftGroupService.logEntries("_admin").size());
    }

    @Test
    public void updateProcessorLoadBalancing() {
        AdminReplicationGroup replicationGroup = givenReplicationGroup("sample", "node1");
        contexts.put("demo", replicationGroup.addContext("demo"));
        contexts.put("demo2", replicationGroup.addContext("demo2"));
        testSubject.updateProcessorLoadBalancing(ProcessorLBStrategy.newBuilder()
                                                                    .setContext("demo2")
                                                                    .setProcessor("the-processor")
                                                                    .setStrategy("the-strategy")
                                                                    .build());


        assertEquals(1, fakeRaftGroupService.logEntries("sample").size());
        assertEquals(1, fakeRaftGroupService.logEntries("_admin").size());
    }

    @Test
    public void deleteUser() {
        AdminReplicationGroup adminGroup = givenReplicationGroup("_admin", "node1");
        contexts.put("_admin", adminGroup.addContext("_admin"));
        AdminReplicationGroup replicationGroup = givenReplicationGroup("sample", "node1");
        contexts.put("demo", replicationGroup.addContext("demo"));
        contexts.put("demo2", replicationGroup.addContext("demo2"));
        givenUser("demoUser", "demo2", "USE_CONTEXT");

        testSubject.deleteUser(io.axoniq.axonserver.grpc.internal.User.newBuilder().setName("demoUser").build());
        assertEquals(1, fakeRaftGroupService.logEntries("sample").size());
        assertEquals(1, fakeRaftGroupService.logEntries("_admin").size());
    }

    @Test
    public void deleteUserWildCardRoles() {
        AdminReplicationGroup adminGroup = givenReplicationGroup("_admin", "node1");
        contexts.put("_admin", adminGroup.addContext("_admin"));
        AdminReplicationGroup replicationGroup = givenReplicationGroup("sample", "node1");
        contexts.put("demo", replicationGroup.addContext("demo"));
        contexts.put("demo2", replicationGroup.addContext("demo2"));
        givenUser("demoUser", "*", "READ");
        testSubject.deleteUser(io.axoniq.axonserver.grpc.internal.User.newBuilder().setName("demoUser").build());
        assertEquals(2, fakeRaftGroupService.logEntries("sample").size());
        assertEquals(2, fakeRaftGroupService.logEntries("_admin").size());
    }

    @Test
    public void deleteApplication() {
        AdminReplicationGroup adminGroup = givenReplicationGroup("_admin", "node1");
        contexts.put("_admin", adminGroup.addContext("_admin"));
        AdminReplicationGroup replicationGroup = givenReplicationGroup("sample", "node1");
        contexts.put("demo", replicationGroup.addContext("demo"));
        contexts.put("demo2", replicationGroup.addContext("demo2"));
        givenApplication("demoApplication", "demo2", "USE_CONTEXT");

        testSubject.deleteApplication(io.axoniq.axonserver.grpc.internal.Application.newBuilder()
                                                                                    .setName("demoApplication")
                                                                                    .build());
        assertEquals(1, fakeRaftGroupService.logEntries("sample").size());
        assertEquals(1, fakeRaftGroupService.logEntries("_admin").size());
    }

    @Test
    public void deleteApplicationWildCardRoles() {
        AdminReplicationGroup adminGroup = givenReplicationGroup("_admin", "node1");
        contexts.put("_admin", adminGroup.addContext("_admin"));
        AdminReplicationGroup replicationGroup = givenReplicationGroup("sample", "node1");
        contexts.put("demo", replicationGroup.addContext("demo"));
        contexts.put("demo2", replicationGroup.addContext("demo2"));
        givenApplication("demoApplication", "*", "USE_CONTEXT");

        testSubject.deleteApplication(io.axoniq.axonserver.grpc.internal.Application.newBuilder()
                                                                                    .setName("demoApplication")
                                                                                    .build());
        assertEquals(2, fakeRaftGroupService.logEntries("sample").size());
        assertEquals(2, fakeRaftGroupService.logEntries("_admin").size());
    }

    private void givenApplication(String s) {
        AdminApplicationContext adminApplicationContext = new AdminApplicationContext(s,
                                                                                      Collections
                                                                                              .singletonList(new AdminApplicationContextRole(
                                                                                                      "USE_CONTEXT")));
        applications.put("demoApplication",
                         new AdminApplication("demoApplication", "demoApplication", "", "", adminApplicationContext));
    }

    @NotNull
    private AdminReplicationGroup givenReplicationGroup(String name, String... nodes) {
        AdminReplicationGroup adminReplicationGroup = new AdminReplicationGroup();
        adminReplicationGroup.setName(name);
        for (String node : nodes) {
            AdminReplicationGroupMember member1 = new AdminReplicationGroupMember();
            ClusterNode node1 = givenNode(node);
            member1.setClusterNode(node1);
            member1.setClusterNodeLabel(node1.getName());
            member1.setRole(Role.PRIMARY);
            adminReplicationGroup.addMember(member1);
            node1.getReplicationGroups().add(member1);
        }

        fakeRaftGroupService.addReplicationGroup(name, nodes);
        replicationGroups.put(adminReplicationGroup.getName(), adminReplicationGroup);

        return adminReplicationGroup;
    }

    @NotNull
    private ClusterNode givenNode(String name) {
        return nodes.computeIfAbsent(name, n -> new ClusterNode(n, n, n, 1, 2, 3));
    }

    private ReplicationGroup createReplicationGroupGrpc(String twice, List<String> asList) {
        return ReplicationGroup.newBuilder().setName(twice).addAllMembers(asList.stream()
                                                                                .map(n -> ReplicationGroupMember
                                                                                        .newBuilder()
                                                                                        .setNodeName(n)
                                                                                        .build())
                                                                                .collect(
                                                                                        Collectors.toList())).build();
    }

    private void givenApplication(String myApplication, String context, String role) {
        AdminApplication application = new AdminApplication(myApplication);
        application.setHashedToken(myApplication);
        application.setTokenPrefix(myApplication);
        application.addContext(new AdminApplicationContext(context,
                                                           Collections
                                                                   .singletonList(new AdminApplicationContextRole(role))));

        applications.put(myApplication, application);
    }

    private void givenUser(String username, String context, String role) {
        Set<UserRole> roles = Collections.singleton(new UserRole(context, role));
        users.put(username, new User(username, username, roles));
    }
}