package io.axoniq.axonserver.enterprise.cluster;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.access.application.ApplicationController;
import io.axoniq.axonserver.access.user.UserController;
import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.config.SystemInfoProvider;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.cluster.internal.RemoteConnection;
import io.axoniq.axonserver.enterprise.context.ContextController;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.jpa.ContextClusterNode;
import io.axoniq.axonserver.enterprise.jpa.Payload;
import io.axoniq.axonserver.enterprise.taskscheduler.JacksonTaskPayloadSerializer;
import io.axoniq.axonserver.enterprise.taskscheduler.TaskPayloadSerializer;
import io.axoniq.axonserver.enterprise.taskscheduler.TaskPublisher;
import io.axoniq.axonserver.enterprise.taskscheduler.TransientException;
import io.axoniq.axonserver.enterprise.taskscheduler.task.*;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.ContextMemberConverter;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.grpc.internal.*;
import junit.framework.TestCase;
import org.junit.*;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.*;
import org.springframework.context.ApplicationEventPublisher;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class LocalRaftConfigServiceTest {
    private LocalRaftConfigService testSubject;
    private AdminDB adminDB;
    private RaftNode adminNode;
    private FakeRaftGroupService fakeRaftGroupService = new FakeRaftGroupService();
    private TaskPublisher taskPublisher;
    private Map<String, Set<String>> scheduledTasks = new ConcurrentHashMap<>();
    private Map<String, Set<Object>> scheduledPayloadsTasks= new ConcurrentHashMap<>();
    private ClusterController clusterController;
    private ApplicationEventPublisher applicationEventPublisher;
    private TaskPayloadSerializer serializer = new JacksonTaskPayloadSerializer();

    private ClusterNode createNode(String name) {
        return new ClusterNode(name, name, name, 1, 2,3);
    }

    private class AdminDB {
        private Map<String,io.axoniq.axonserver.enterprise.jpa.Context> contextMap = new ConcurrentHashMap<>();
        private Map<String,ClusterNode> nodeMap = new ConcurrentHashMap<>();

        public void addContext(String name, String... nodes) {
            io.axoniq.axonserver.enterprise.jpa.Context context = contextMap.computeIfAbsent(name, io.axoniq.axonserver.enterprise.jpa.Context::new);
            for (String node : nodes) {
                nodeMap.computeIfAbsent(node, LocalRaftConfigServiceTest.this::createNode).addContext(context,
                                                                                                      node + "/"
                                                                                                              + context,
                                                                                                      Role.PRIMARY);
            }
        }


        public void applyEntry(String entryType, byte[] value) {
            if( entryType.equals(ContextConfiguration.class.getName())) {
                try {
                    ContextConfiguration contextConfiguration = ContextConfiguration.parseFrom(value);
                    io.axoniq.axonserver.enterprise.jpa.Context context = contextMap.get(contextConfiguration.getContext());
                    if( contextConfiguration.getNodesCount() == 0) {
                        contextMap.remove(context.getName());
                        return;
                    }

                    if( context == null) {
                        context = new io.axoniq.axonserver.enterprise.jpa.Context(contextConfiguration.getContext());
                        contextMap.put(context.getName(), context);
                    }
                    Map<String, ClusterNode> currentNodes = new HashMap<>();
                    context.getNodes().forEach(n -> currentNodes.put(n.getClusterNode().getName(), n.getClusterNode()));
                    Map<String, NodeInfoWithLabel> newNodes = new HashMap<>();
                    contextConfiguration.getNodesList().forEach(n -> newNodes.put(n.getNode().getNodeName(), n));

                    Map<String, ClusterNode> clusterInfoMap = new HashMap<>();
                    for (NodeInfoWithLabel nodeInfo : contextConfiguration.getNodesList()) {
                        String nodeName = nodeInfo.getNode().getNodeName();
                        ClusterNode clusterNode = nodeMap.get(nodeName);
                        if( clusterNode == null) clusterNode = createNode(nodeName);
                        clusterInfoMap.put(nodeName, clusterNode);
                    }

                    io.axoniq.axonserver.enterprise.jpa.Context finalContext = context;
                    currentNodes.forEach((node, clusterNode) -> {
                        if( !newNodes.containsKey(node)) {
                            clusterNode.removeContext(finalContext.getName());
                        }
                    });
                    newNodes.forEach((node, nodeInfo) -> {
                        if( !currentNodes.containsKey(node)) {
                            clusterInfoMap.get(node).addContext(finalContext,
                                                                nodeInfo.getLabel(),
                                                                nodeInfo.getRole());
                        }
                    });

                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private class GroupDB {

        private Map<String, Node> nodes = new ConcurrentHashMap<>();

        private GroupDB(io.axoniq.axonserver.enterprise.jpa.Context context) {
            context.getNodes().forEach(ccn -> nodes.put(ccn.getClusterNodeLabel(), Node.newBuilder()
                                                                                       .setNodeName(ccn.getClusterNode()
                                                                                                          .getName())
                                                                                       .setNodeId(ccn.getClusterNodeLabel())
                                                                                       .setRole(ccn.getRole())
                                                                                       .build()));
        }

        public GroupDB() {

        }
    }

    private class FakeRaftGroupService implements RaftGroupService {
        private Map<String, GroupDB> groupDBs = new ConcurrentHashMap<>();

        void add(io.axoniq.axonserver.enterprise.jpa.Context context) {
            groupDBs.put(context.getName(),new GroupDB(context));
        }

        @Override
        public CompletableFuture<ContextUpdateConfirmation> addNodeToContext(String context, Node node) {
            groupDBs.get(context).nodes.put(node.getNodeId(), node);
            ContextUpdateConfirmation.Builder contextUpdateConfirmation = ContextUpdateConfirmation.newBuilder()
                    .setSuccess(true);
            groupDBs.get(context).nodes.forEach((id, n) ->
                                                        contextUpdateConfirmation
                                                                .addMembers(ContextMemberConverter.asContextMember(n)));

            return CompletableFuture.completedFuture(contextUpdateConfirmation.build());
        }

        @Override
        public CompletableFuture<Void> getStatus(Consumer<Context> contextConsumer) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<ContextConfiguration> configuration(String context) {
            return null;
        }

        @Override
        public CompletableFuture<ContextConfiguration> initContext(String context, List<Node> nodes) {
            GroupDB groupDB = new GroupDB();
            nodes.forEach(n -> groupDB.nodes.put(n.getNodeId(), n));
            groupDBs.put(context, groupDB);
            ContextConfiguration contextConfiguration = ContextConfiguration.newBuilder()
                                                                            .setContext(context)
                                                                            .addAllNodes(nodes.stream()
                                                                                              .map(n -> NodeInfoWithLabel
                                                                                                      .newBuilder()
                                                                                                      .setLabel(n.getNodeName())
                                                                                                      .setNode(NodeInfo.newBuilder()
                                                                                                                       .setNodeName(
                                                                                                                               n.getNodeName()))
                                                                                                      .setRole(n.getRole())
                                                                                                      .build())
                                                                                              .collect(Collectors
                                                                                                               .toList()))
                                                                            .build();
            return CompletableFuture.completedFuture(contextConfiguration);
        }

        @Override
        public CompletableFuture<ContextUpdateConfirmation> deleteNode(String context, String node) {
            groupDBs.get(context).nodes.remove(node);
            ContextUpdateConfirmation.Builder contextUpdateConfirmation = ContextUpdateConfirmation.newBuilder()
                                                                                                   .setSuccess(true);
            groupDBs.get(context).nodes.forEach((id, n) ->
                                                        contextUpdateConfirmation
                                                                .addMembers(ContextMemberConverter.asContextMember(n)));

            return CompletableFuture.completedFuture(contextUpdateConfirmation.build());
        }

        @Override
        public CompletableFuture<Void> updateApplication(ContextApplication application) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> deleteApplication(ContextApplication application) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> updateUser(ContextUser user) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> deleteUser(ContextUser user) {
            return null;
        }

        @Override
        public CompletableFuture<Void> updateLoadBalancingStrategy(String context,
                                                                   LoadBalanceStrategy loadBalancingStrategy) {
            return null;
        }

        @Override
        public CompletableFuture<Void> updateProcessorLoadBalancing(String context,
                                                                    ProcessorLBStrategy processorLBStrategy) {
            return null;
        }

        @Override
        public CompletableFuture<Void> deleteLoadBalancingStrategy(String context,
                                                                   LoadBalanceStrategy loadBalancingStrategy) {
            return null;
        }

        @Override
        public CompletableFuture<Void> deleteContext(String context, boolean preserveEventStore) {
            groupDBs.remove(context);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> appendEntry(String context, String name, byte[] toByteArray) {
            if( context.equals("_admin")) {
                adminDB.applyEntry(name, toByteArray);
            }
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> transferLeadership(String context) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> prepareDeleteNodeFromContext(String context, String node) {
            return CompletableFuture.completedFuture(null);
        }
    }

    @Before
    public void setUp() {
        GrpcRaftController grpcRaftController = mock(GrpcRaftController.class);
        adminNode = mock(RaftNode.class);

        adminDB = new AdminDB();
        adminDB.addContext("sample", "node1");
        adminDB.addContext("_admin", "node1", "node2");
        adminDB.contextMap.forEach((key,context) -> fakeRaftGroupService.add(context));

        when(adminNode.appendEntry(any(), any())).then((Answer<CompletableFuture<Void>>) invocationOnMock -> {
            adminDB.applyEntry(invocationOnMock.getArgument(0), invocationOnMock.getArgument(1));
                return CompletableFuture.completedFuture(null);
        });

        when(grpcRaftController.getRaftNode("_admin")).thenReturn(adminNode);
        ContextController contextcontroller = mock(ContextController.class);
        clusterController = mock(ClusterController.class,  Mockito.RETURNS_DEEP_STUBS);
        applicationEventPublisher = mock(ApplicationEventPublisher.class);

        RaftGroupServiceFactory raftGroupServiceFactory = mock(RaftGroupServiceFactory.class);

        when(raftGroupServiceFactory.getRaftGroupService(anyString())).thenReturn(fakeRaftGroupService);
        when(raftGroupServiceFactory.getRaftGroupServiceForNode(anyString())).thenReturn(fakeRaftGroupService);
        when(raftGroupServiceFactory.getRaftGroupServiceForNode(any(ClusterNode.class))).thenReturn(fakeRaftGroupService);
        ApplicationController applicationController = mock(ApplicationController.class);
        MessagingPlatformConfiguration messagingPlatformConfiguration = new MessagingPlatformConfiguration(new SystemInfoProvider() {
            @Override
            public int getPort() {
                return 0;
            }

            @Override
            public String getHostName()  {
                return "localhost";
            }
        });

        when(clusterController.nodes()).then((Answer<Stream<ClusterNode>>) invocationOnMock -> adminDB.nodeMap.values().stream());

        when(clusterController.getNode(anyString())).then((Answer<ClusterNode>) invocationOnMock -> {
            String name = invocationOnMock.getArgument(0);
            return adminDB.nodeMap.get(name);
        });

        when(contextcontroller
                     .getContext(anyString()))
                .then((Answer<io.axoniq.axonserver.enterprise.jpa.Context>) invocationOnMock -> {
            String name = invocationOnMock.getArgument(0);
            return adminDB.contextMap.get(name);
        });
        RaftGroup fakeRaftGroup = mock(RaftGroup.class);
        when(fakeRaftGroup.localNode()).thenReturn(adminNode);
        when(grpcRaftController.initRaftGroup(anyString(), anyString(), anyString())).thenAnswer((Answer<RaftGroup>)invocationOnMock -> {
            GroupDB groupDB = new GroupDB();
            groupDB.nodes.put(invocationOnMock.getArgument(1), invocationOnMock.getArgument(2));
            fakeRaftGroupService.groupDBs.put(invocationOnMock.getArgument(0), groupDB);
            return fakeRaftGroup;
        });
        when(grpcRaftController.waitForLeader(any())).thenReturn(adminNode);
        when(adminNode.addNode(any())).thenReturn(CompletableFuture.completedFuture(null));
        UserController userController = mock(UserController.class);
        taskPublisher = new TaskPublisher(null, null) {
            @Override
            public CompletableFuture<Void> publishScheduledTask(String context, String taskHandler, Object payload,
                                                                Duration delay) {
                scheduledTasks.computeIfAbsent(context, c -> new CopyOnWriteArraySet<>()).add(taskHandler);
                scheduledPayloadsTasks.computeIfAbsent(taskHandler, c -> new CopyOnWriteArraySet<>()).add(payload);
                return CompletableFuture.completedFuture(null);
            }
        };
        testSubject = new LocalRaftConfigService(grpcRaftController,
                                                 contextcontroller,
                                                 clusterController,
                                                 raftGroupServiceFactory,
                                                 applicationController,
                                                 userController,
                                                 messagingPlatformConfiguration,
                                                 taskPublisher);
    }

    @Test
    public void addNodeToContext() {
        testSubject.addNodeToContext("sample", "node2", Role.PRIMARY);
        assertTrue(adminDB.contextMap.get("sample").getNodeNames().contains("node2"));
        assertTrue(fakeRaftGroupService.groupDBs.get("sample").nodes
                           .values().stream().anyMatch(n -> n.getNodeName().equals("node2")));
    }

    @Test
    public void addNodeWithOtherRoleToContext() {
        testSubject.addNodeToContext("sample", "node2", Role.ACTIVE_BACKUP);
        assertTrue(adminDB.contextMap.get("sample").getNodeNames().contains("node2"));
        ContextClusterNode ccn = adminDB.contextMap.get("sample")
                                                   .getNodes()
                                                   .stream()
                                                   .filter(c -> c.getClusterNode().getName().equals("node2"))
                                                   .findFirst()
                                                   .orElse(null);

        assertNotNull(ccn);
        assertEquals(Role.ACTIVE_BACKUP, ccn.getRole());

        Optional<Node> node = fakeRaftGroupService.groupDBs.get("sample").nodes.values().stream().filter(n -> n
                .getNodeName().equals("node2")).findFirst();

        assertTrue(node.isPresent());
        assertEquals(Role.ACTIVE_BACKUP, node.get().getRole());
    }

    @Test
    public void addNodeToNonExistingContext() {
        try {
            testSubject.addNodeToContext("myContext", "node1", Role.PRIMARY);
            fail("Expect exception");
        } catch( MessagingPlatformException mpe) {
            assertEquals(ErrorCode.CONTEXT_NOT_FOUND, mpe.getErrorCode());
        }
    }

    @Test
    public void addNonExistingNodeToContext() {
        try {
            testSubject.addNodeToContext("sample", "node3", Role.PRIMARY);
            fail("Expect exception");
        } catch( MessagingPlatformException mpe) {
            assertEquals(ErrorCode.NO_SUCH_NODE, mpe.getErrorCode());
        }
    }

    @Test
    public void addExistingMemberToContext() {
        testSubject.addNodeToContext("sample", "node1", Role.PRIMARY);
    }

    @Test
    public void deleteContext() {
        testSubject.deleteContext("sample");
        assertNull( adminDB.contextMap.get("sample"));
    }

    @Test
    public void deleteAdminContext() {
        try {
            testSubject.deleteContext("_admin");
            fail("Expect exception");
        } catch(MessagingPlatformException mpe) {
            assertEquals(ErrorCode.CANNOT_DELETE_INTERNAL_CONTEXT, mpe.getErrorCode());
        }
    }

    @Test
    public void deleteNonExistingContext() {
        testSubject.deleteContext("demo");
    }

    @Test
    public void deleteNodeFromContext() {
        testSubject.deleteNodeFromContext("_admin", "node2");
        assertEquals(1, adminDB.contextMap.get("_admin").getNodeNames().size());
        try {
            testSubject.deleteNodeFromContext("_admin", "node1");
            fail("Expect exception");
        } catch(MessagingPlatformException mpe) {
            assertEquals(ErrorCode.CANNOT_REMOVE_LAST_NODE, mpe.getErrorCode());
        }
    }

    @Test
    public void distributeLicense() {
        testSubject.distributeLicense("myLicense".getBytes());
        assertEquals(1, scheduledTasks.size());
        Set<String> adminTasks = scheduledTasks.getOrDefault("_admin", Collections.emptySet());
        assertTrue(adminTasks.contains(PrepareUpdateLicenseTask.class.getName()));
    }

    @Test
    public void prepareUpdateLicenseTask() {

        PrepareUpdateLicenseTask prepareUpdateLicenseTask = new PrepareUpdateLicenseTask(taskPublisher, serializer, clusterController);
        prepareUpdateLicenseTask.execute("myLicense".getBytes());


        Set<UpdateLicenseTaskPayload> scheduledPayloads = scheduledPayloadsTasks.getOrDefault(UpdateLicenseTask.class.getName(), Collections.emptySet())
                .stream()
                .map(it -> (UpdateLicenseTaskPayload) serializer.deserialize((Payload) it))
                .collect(Collectors.toSet());

        TestCase.assertEquals(2, scheduledPayloads.size());
        TestCase.assertEquals(1, scheduledPayloads.stream().filter(it -> it.getNodeName().equals("node1")).count());
        TestCase.assertEquals(1, scheduledPayloads.stream().filter(it -> it.getNodeName().equals("node2")).count());
    }

    @Test
    public void updateLicenseTaskAdminNode() {
        when(clusterController.getMe().getName()).thenReturn("adminNode");

        UpdateLicenseTaskPayload updateLicenseTaskPayload = new UpdateLicenseTaskPayload("adminNode","myLicense".getBytes());

        UpdateLicenseTask updateLicenseTask = new UpdateLicenseTask(serializer, clusterController, applicationEventPublisher);
        updateLicenseTask.execute(serializer.serialize(updateLicenseTaskPayload));

        verify(applicationEventPublisher, times(1)).publishEvent(any(ClusterEvents.LicenseUpdated.class));
    }

    @Test
    public void updateLicenseTaskRemoteConnectedNode() {
        when(clusterController.getMe().getName()).thenReturn("adminNode");

        RemoteConnection remoteConnection = mock(RemoteConnection.class, RETURNS_DEEP_STUBS);
        when(remoteConnection.isConnected()).thenReturn(true);
        when(remoteConnection.getClusterNode().getName()).thenReturn("remoteNode");

        when(clusterController.getRemoteConnections()).thenReturn(Arrays.asList(remoteConnection));

        UpdateLicenseTaskPayload updateLicenseTaskPayload = new UpdateLicenseTaskPayload("remoteNode","myLicense".getBytes());

        UpdateLicenseTask updateLicenseTask = new UpdateLicenseTask(serializer, clusterController, applicationEventPublisher);
        updateLicenseTask.execute(serializer.serialize(updateLicenseTaskPayload));

        verify(remoteConnection, times(1)).publish(any(ConnectorCommand.class));

        ArgumentCaptor<ConnectorCommand> captor = ArgumentCaptor.forClass(ConnectorCommand.class);

        verify(remoteConnection, times(1)).publish(captor.capture());

        String licenseContent = new String(captor.getValue().getDistributeLicense().getLicense().toByteArray());
        assertEquals(licenseContent,"myLicense");
    }

    @Test
    public void updateLicenseTaskRemoteDisconnectedNode() {
        when(clusterController.getMe().getName()).thenReturn("adminNode");

        RemoteConnection remoteConnection = mock(RemoteConnection.class, RETURNS_DEEP_STUBS);

        when(remoteConnection.isConnected()).thenReturn(false);

        when(clusterController.getRemoteConnections()).thenReturn(Arrays.asList(remoteConnection));

        UpdateLicenseTaskPayload updateLicenseTaskPayload = new UpdateLicenseTaskPayload("remoteNode","myLicense".getBytes());

        UpdateLicenseTask updateLicenseTask = new UpdateLicenseTask(serializer, clusterController, applicationEventPublisher);

        try {
            updateLicenseTask.execute(serializer.serialize(updateLicenseTaskPayload));
        } catch (TransientException e){
            assertTrue(true);
        }
    }


    @Test
    public void deleteNode() {
        testSubject.deleteNode("node2");
        assertEquals(1, scheduledTasks.size());
        Set<String> adminTasks = scheduledTasks.getOrDefault("_admin", Collections.emptySet());
        assertTrue(adminTasks.contains(PrepareDeleteNodeFromContextTask.class.getName()));
        assertTrue(adminTasks.contains(UnregisterNodeTask.class.getName()));
    }

    @Test
    public void addContext() {
        testSubject.addContext(createContext("second", Arrays.asList("node1", "node2")));
    }

    @Test(expected = Throwable.class)
    public void addContextTwice() {
        testSubject.addContext(createContext("twice", Arrays.asList("node1", "node2")));
        testSubject.addContext(createContext("twice", Arrays.asList("node1", "node2")));
    }

    private Context createContext(String twice, List<String> asList) {
        return Context.newBuilder().setName(twice).addAllMembers(asList.stream().map(n -> ContextMember.newBuilder()
                                                                                                       .setNodeName(n)
                                                                                                       .build())
                                                                       .collect(
                                                                               Collectors.toList())).build();
    }

    @Test
    public void join() {
        when(adminNode.isLeader()).thenReturn(true);
        testSubject.join(NodeInfo.newBuilder().setNodeName("node3").setInternalHostName("node3").setHostName("node3")
                                 .addContexts(ContextRole.newBuilder().setName("sample").setNodeLabel("sample/node3")).build());

    }

    @Test
    public void joinNewContext() {
        when(adminNode.isLeader()).thenReturn(true);
        testSubject.join(NodeInfo.newBuilder().setNodeName("node3").setInternalHostName("node3").setHostName("node3")
                                 .addContexts(ContextRole.newBuilder().setName("sample2").setNodeLabel("sample2/node3")).build());

    }

    @Test
    public void joinAllContexts() {
        when(adminNode.isLeader()).thenReturn(true);
        testSubject.join(NodeInfo.newBuilder().setNodeName("node3").setInternalHostName("node3").setHostName("node3").build());

    }

    @Test
    public void joinNoContexts() {
        when(adminNode.isLeader()).thenReturn(true);
        testSubject.join(NodeInfo.newBuilder().setNodeName("node3").setInternalHostName("node3").setHostName("node3")
                .addContexts(ContextRole.newBuilder().setName("_none")).build());

    }

    @Test
    public void init() {
        adminDB.contextMap.clear();
        adminDB.nodeMap.clear();
        adminDB.nodeMap.put("localhost", createNode("localhost"));
        fakeRaftGroupService.groupDBs.clear();

        testSubject.init(Arrays.asList("default"));
        assertEquals(2, adminDB.contextMap.size());
        assertEquals(2,fakeRaftGroupService.groupDBs.size());

    }

}