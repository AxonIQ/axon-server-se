package io.axoniq.axonserver.enterprise.cluster;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.cluster.grpc.LeaderElectionService;
import io.axoniq.axonserver.cluster.grpc.LogReplicationService;
import io.axoniq.axonserver.cluster.jpa.JpaRaftGroupNode;
import io.axoniq.axonserver.cluster.jpa.JpaRaftGroupNodeRepository;
import io.axoniq.axonserver.cluster.jpa.JpaRaftStateRepository;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.context.ContextController;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.internal.ContextConfiguration;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.grpc.internal.TransactionWithToken;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Controller;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Author: marc
 */
@Controller
public class GrpcRaftController implements SmartLifecycle, ApplicationContextAware {

    public static final String CONFIG_GROUP = "_config";
    private final LogReplicationService logReplicationService;
    private final LeaderElectionService leaderElectionService;
    private final JpaRaftStateRepository raftStateRepository;
    private final RaftServiceFactory raftServiceFactory;
    private final MessagingPlatformConfiguration messagingPlatformConfiguration;
    private LocalEventStore localEventStore;
    private final Map<String,RaftGroup> raftGroupMap = new ConcurrentHashMap<>();
    private boolean running;
    private final ContextController contextController;
    private final JpaRaftGroupNodeRepository raftGroupNodeRepository;
    private ApplicationContext applicationContext;

    public GrpcRaftController(LogReplicationService logReplicationService,
                              LeaderElectionService leaderElectionService,
                              JpaRaftStateRepository raftStateRepository,
                              RaftServiceFactory raftServiceFactory,
                              MessagingPlatformConfiguration messagingPlatformConfiguration,
                              ContextController contextController,
                              JpaRaftGroupNodeRepository raftGroupNodeRepository) {
        this.logReplicationService = logReplicationService;
        this.leaderElectionService = leaderElectionService;
        this.raftStateRepository = raftStateRepository;
        this.raftServiceFactory = raftServiceFactory;
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
        this.contextController = contextController;
        this.raftGroupNodeRepository = raftGroupNodeRepository;
    }


    public void start() {
        localEventStore = applicationContext.getBean(LocalEventStore.class);
        Set<JpaRaftGroupNode> groups = raftGroupNodeRepository.findByNodeId(messagingPlatformConfiguration.getName());
        groups.forEach(context -> createRaftGroup(context.getGroupId(), messagingPlatformConfiguration.getName()));
        running = true;
    }

    @Override
    public void stop() {
        stop(()->{});

    }

    @Override
    public boolean isRunning() {
        return running;
    }

    public RaftGroup initRaftGroup(String groupId) {
        JpaRaftGroupNode jpaRaftGroupNode = new JpaRaftGroupNode();
        jpaRaftGroupNode.setGroupId(groupId);
        jpaRaftGroupNode.setNodeId(messagingPlatformConfiguration.getName());
        jpaRaftGroupNode.setHost(messagingPlatformConfiguration.getFullyQualifiedInternalHostname());
        jpaRaftGroupNode.setPort(messagingPlatformConfiguration.getInternalPort());
        raftGroupNodeRepository.save(jpaRaftGroupNode);
        return createRaftGroup(groupId, messagingPlatformConfiguration.getName());
    }

    public RaftGroup createRaftGroup(String groupId, String nodeId) {
        Set<JpaRaftGroupNode> nodes= raftGroupNodeRepository.findByGroupId(groupId);
        RaftGroup raftGroup = new GrpcRaftGroup(nodeId, nodes, groupId, raftStateRepository);
        logReplicationService.addRaftNode(raftGroup.localNode());
        leaderElectionService.addRaftNode(raftGroup.localNode());
        if( CONFIG_GROUP.equals(groupId)) {
            raftGroup.localNode().registerEntryConsumer(this::processConfigEntry);
        } else {
            raftGroup.localNode().registerEntryConsumer(entry -> processEventEntry(groupId, entry));
        }

        raftGroup.localNode().start();
        raftGroupMap.put(groupId, raftGroup);
        return raftGroup;
    }

    private void processEventEntry(String groupId, Entry e) {
            System.out.println(e.hasSerializedObject());
            if (e.hasSerializedObject()) {
                System.out.println(e.getSerializedObject().getType());
                if (e.getSerializedObject().getType().equals("Append.EVENT")) {
                    TransactionWithToken transactionWithToken = null;
                    try {
                        transactionWithToken = TransactionWithToken.parseFrom(e.getSerializedObject().getData());
                        System.out.println(transactionWithToken.getToken());
                        if (transactionWithToken.getToken() > localEventStore.getLastToken(groupId)) {
                            localEventStore.syncEvents(groupId, transactionWithToken);
                        }
                    } catch (InvalidProtocolBufferException e1) {
                        e1.printStackTrace();
                    }
                }
            }
    }

    private void processConfigEntry(Entry e) {
        if( e.hasSerializedObject()) {
//            if( ContextUpdate.class.getName().equals(e.getSerializedObject().getType()) ) {
//                try {
//                    ContextUpdate contextUpdate = ContextUpdate.parseFrom(e.getSerializedObject().getData());
//                    boolean exists = false;
//                    if( contextController.getContext(contextUpdate.getName()) != null) {
//                        contextController.deleteContext(contextUpdate.getName(), false);
//                        exists = true;
//                    }
//                    switch ( contextUpdate.getAction()) {
//
//                        case MERGE_CONTEXT:
//                            contextController.addContext(contextUpdate.getName(),
//                                                         contextUpdate.getNodesList().stream().map(n -> new NodeRoles(n.getName(), false, false)).collect(
//                                                                 Collectors.toList()),
//                                                         false);
//
//                            if( !exists) {
//                                JpaRaftGroupNode jpaRaftGroupNode = new JpaRaftGroupNode();
//                                jpaRaftGroupNode.setGroupId(contextUpdate.getName());
//                                jpaRaftGroupNode.setNodeId(messagingPlatformConfiguration.getName());
//                                jpaRaftGroupNode.setHost(messagingPlatformConfiguration.getFullyQualifiedInternalHostname());
//                                jpaRaftGroupNode.setPort(messagingPlatformConfiguration.getInternalPort());
//                                raftGroupNodeRepository.save(jpaRaftGroupNode);
//
//                                RaftGroup group = createRaftGroup(contextUpdate.getName(),
//                                                                  messagingPlatformConfiguration.getName());
//                                raftGroupMap.put(CONFIG_GROUP, group);
//
//                            }
//                            break;
//                        case DELETE_CONTEXT:
//                            break;
//                        case NODES_UPDATED:
//                            break;
//                        case UNRECOGNIZED:
//                            break;
//                    }
//                } catch (Throwable e1) {
//                    e1.printStackTrace();
//                }
//            }
        }
    }

    public Collection<String> getContexts() {
        return raftGroupMap.keySet();
    }

    public String getStorageMaster(String s) {
        if( raftGroupMap.containsKey(s))
            return raftGroupMap.get(s).localNode().getLeader();

        return null;
    }

    public RaftNode getStorageRaftNode(String context) {
        return raftGroupMap.get(context).localNode();
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void stop(Runnable callback) {
        raftGroupMap.values().forEach(r -> r.localNode().stop());
        callback.run();
        running=false;

    }

    @Override
    public int getPhase() {
        return 100;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    public void addContext(String context, List<String> nodes) throws ExecutionException, InterruptedException {
        RaftGroup configGroup =  raftGroupMap.get(CONFIG_GROUP);
        if( configGroup == null) {
//            contextController.addContext(CONFIG_GROUP, Collections.singletonList(new NodeRoles(messagingPlatformConfiguration.getName(), false,false)),false);
            JpaRaftGroupNode jpaRaftGroupNode = new JpaRaftGroupNode();
            jpaRaftGroupNode.setGroupId(CONFIG_GROUP);
            jpaRaftGroupNode.setNodeId(messagingPlatformConfiguration.getName());
            jpaRaftGroupNode.setHost(messagingPlatformConfiguration.getFullyQualifiedInternalHostname());
            jpaRaftGroupNode.setPort(messagingPlatformConfiguration.getInternalPort());
            raftGroupNodeRepository.save(jpaRaftGroupNode);

            configGroup = createRaftGroup(CONFIG_GROUP, messagingPlatformConfiguration.getName());
            raftGroupMap.put(CONFIG_GROUP, configGroup);

//            ContextUpdate contextUpdate = ContextUpdate.newBuilder()
//                                                       .setName(CONFIG_GROUP)
//                                                       .setAction(ContextAction.MERGE_CONTEXT)
//                                                       .addNodes(NodeRole.newBuilder().setName(messagingPlatformConfiguration.getName()).build())
//                                                       .build();

            while (configGroup.localNode().getLeader() == null) {
                Thread.sleep(100);
            }
//            configGroup.localNode().appendEntry(ContextUpdate.class.getName(), contextUpdate.toByteArray()).get();

        }

//        Iterable<? extends NodeRole> nodeRoles = nodes.stream().map(n -> NodeRole.newBuilder().setName(n).build()).collect(
//                Collectors.toList());
//        ContextUpdate contextUpdate = ContextUpdate.newBuilder()
//                                                   .setName(context)
//                                                   .setAction(ContextAction.MERGE_CONTEXT)
//                                                   .addAllNodes(nodeRoles)
//                                                   .build();
//
//        String leader = configGroup.localNode().getLeader();
//        if( leader == null) throw new RuntimeException("No Leader for CONFIG group");
//        if( messagingPlatformConfiguration.getName().equals(leader)) {
//            configGroup.localNode().appendEntry(ContextUpdate.class.getName(), contextUpdate.toByteArray()).get();
//        } else {
//            clusterController.publishTo(leader, ConnectorCommand.newBuilder().setContext(contextUpdate).build());
//        }
    }

    public void init(List<String> contexts) {
        RaftGroup configGroup = initRaftGroup(CONFIG_GROUP);
        RaftNode leader = waitForLeader(configGroup);
        Node me = Node.newBuilder().setNodeId(messagingPlatformConfiguration.getName())
                .setHost(messagingPlatformConfiguration.getFullyQualifiedInternalHostname())
                .setPort(messagingPlatformConfiguration.getInternalPort())
                .build();

        leader.addNode(me);
        NodeInfo nodeInfo = NodeInfo.newBuilder()
                                    .setGrpcInternalPort(messagingPlatformConfiguration.getInternalPort())
                                    .setNodeName(messagingPlatformConfiguration.getName())
                                    .setInternalHostName(messagingPlatformConfiguration.getFullyQualifiedInternalHostname())
                                    .setGrpcPort(messagingPlatformConfiguration.getPort())
                                    .setHttpPort(messagingPlatformConfiguration.getHttpPort())
                                    .setHostName(messagingPlatformConfiguration.getFullyQualifiedHostname())
                                    .build();
        ContextConfiguration contextConfiguration = ContextConfiguration.newBuilder()
                                                                        .setContext(CONFIG_GROUP)
                                                                        .addNodes(nodeInfo)
                                                                        .build();
        leader.appendEntry(ContextConfiguration.class.getName(), contextConfiguration.toByteArray());

        contexts.forEach(c -> {
            RaftGroup group = initRaftGroup(c);
            RaftNode groupLeader = waitForLeader(group);
            groupLeader.addNode(me);
            ContextConfiguration groupConfiguration = ContextConfiguration.newBuilder()
                                                                            .setContext(c)
                                                                            .addNodes(nodeInfo)
                                                                            .build();
            leader.appendEntry(ContextConfiguration.class.getName(), groupConfiguration.toByteArray());
        });
    }

    private RaftNode waitForLeader(RaftGroup group) {
        while (! group.localNode().isLeader()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupt while waiting to become leader");
            }
        }
        return group.localNode();
    }

    public void join(NodeInfo nodeInfo) {
        RaftGroup config = raftGroupMap.get(CONFIG_GROUP);
        if( config == null) {
            throw new RuntimeException("Node is not a member of configuration group");
        }
        if(! config.localNode().isLeader()) {
            throw new RuntimeException("Node is not leader for configuration, leader = " + config.localNode().getLeader());
        }

        List<String> contexts = nodeInfo.getContextsList().stream().map(c -> c.getName()).collect(Collectors.toList());
        if( contexts.isEmpty()) {
            contexts = contextController.getContexts().map(c -> c.getName()).collect(Collectors.toList());
        }

        Node me = Node.newBuilder().setNodeId(nodeInfo.getNodeName())
                      .setHost(nodeInfo.getInternalHostName())
                      .setPort(nodeInfo.getGrpcInternalPort())
                      .build();
        contexts.forEach(c -> {
            getLeaderService(c).addNodeToContext(me);

            config.localNode().appendEntry(ContextConfiguration.class.getName(), createContextConfigBuilder(c).addNodes(nodeInfo).build().toByteArray());
        });
    }

    private RaftLeaderService getLeaderService(String c) {
        RaftGroup raftGroup = raftGroupMap.get(c);
        if( raftGroup == null) {
            throw new UnsupportedOperationException("Unable to get leader for context, group is not available on this server");
        }

        return raftServiceFactory.getRaftService( raftGroup);
    }

    private ContextConfiguration.Builder createContextConfigBuilder(String c) {
        ContextConfiguration.Builder groupConfigurationBuilder = ContextConfiguration.newBuilder()
                                                                                     .setContext(c);
        contextController.getContext(c).getAllNodes().forEach(n -> groupConfigurationBuilder.addNodes(n.getClusterNode().toNodeInfo()));
        return groupConfigurationBuilder;
    }
}
