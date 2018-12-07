package io.axoniq.axonserver.enterprise.cluster;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.cluster.grpc.RaftGroupManager;
import io.axoniq.axonserver.cluster.jpa.JpaRaftGroupNode;
import io.axoniq.axonserver.cluster.jpa.JpaRaftGroupNodeRepository;
import io.axoniq.axonserver.cluster.jpa.JpaRaftStateRepository;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.context.ContextController;
import io.axoniq.axonserver.grpc.Confirmation;
import io.axoniq.axonserver.grpc.cluster.Config;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.internal.Context;
import io.axoniq.axonserver.grpc.internal.ContextConfiguration;
import io.axoniq.axonserver.grpc.internal.ContextMember;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.grpc.internal.TransactionWithToken;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.SmartLifecycle;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;

import java.util.ArrayList;
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
public class GrpcRaftController implements SmartLifecycle, ApplicationContextAware, RaftGroupManager {

    private final Logger logger = LoggerFactory.getLogger(GrpcRaftController.class);
    public static final String ADMIN_GROUP = "_admin";
    private final JpaRaftStateRepository raftStateRepository;
    private final RaftServiceFactory raftServiceFactory;
    private final MessagingPlatformConfiguration messagingPlatformConfiguration;
    private LocalEventStore localEventStore;
    private final Map<String,RaftGroup> raftGroupMap = new ConcurrentHashMap<>();
    private boolean running;
    private final ContextController contextController;
    private final JpaRaftGroupNodeRepository raftGroupNodeRepository;
    private final ApplicationEventPublisher eventPublisher;
    private ApplicationContext applicationContext;

    public GrpcRaftController(JpaRaftStateRepository raftStateRepository,
                              RaftServiceFactory raftServiceFactory,
                              MessagingPlatformConfiguration messagingPlatformConfiguration,
                              ContextController contextController,
                              JpaRaftGroupNodeRepository raftGroupNodeRepository,
                              ApplicationEventPublisher eventPublisher) {
        this.raftStateRepository = raftStateRepository;
        this.raftServiceFactory = raftServiceFactory;
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
        this.contextController = contextController;
        this.raftGroupNodeRepository = raftGroupNodeRepository;
        this.eventPublisher = eventPublisher;
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

        if( ADMIN_GROUP.equals(groupId)) {
            raftGroup.localNode().registerEntryConsumer(this::processConfigEntry);
        } else {
            raftGroup.localNode().registerEntryConsumer(entry -> processEventEntry(groupId, entry));
            localEventStore.initContext(groupId, false);
        }
        raftGroup.localNode().registerEntryConsumer(entry -> processNewConfiguration(groupId, entry));

        raftGroup.localNode().start();
        raftGroupMap.put(groupId, raftGroup);
        return raftGroup;
    }

    private void processEventEntry(String groupId, Entry e) {

            if (e.hasSerializedObject()) {
                logger.warn("{}: received type: {}", groupId, e.getSerializedObject().getType());
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

    private void processNewConfiguration(String groupId, Entry e) {
        if( e.hasNewConfiguration()) {
            Config configuration = e.getNewConfiguration();
            logger.warn("{}: received config: {}", groupId, configuration);

            Set<JpaRaftGroupNode> oldNodes = raftGroupNodeRepository.findByGroupId(groupId);
            raftGroupNodeRepository.deleteAll(oldNodes);
            configuration.getNodesList().forEach(node -> {
                JpaRaftGroupNode jpaRaftGroupNode = new JpaRaftGroupNode();
                jpaRaftGroupNode.setGroupId(groupId);
                jpaRaftGroupNode.setNodeId(node.getNodeId());
                jpaRaftGroupNode.setHost(node.getHost());
                jpaRaftGroupNode.setPort(node.getPort());
                raftGroupNodeRepository.save(jpaRaftGroupNode);
            });

        }
    }

    private void processConfigEntry(Entry e) {
        if( e.hasSerializedObject()) {
            logger.warn("{}: received type: {}", ADMIN_GROUP, e.getSerializedObject().getType());
            if( ContextConfiguration.class.getName().equals(e.getSerializedObject().getType())) {
                try {
                    ContextConfiguration contextConfiguration = ContextConfiguration.parseFrom(e.getSerializedObject().getData());
                    logger.warn("{}: received data: {}", ADMIN_GROUP, contextConfiguration);
                    contextController.updateContext(contextConfiguration);
                    eventPublisher.publishEvent(new ClusterEvents.AxonServerInstanceDisconnected("test"));
                } catch (Exception e1) {
                    logger.warn("{}: Failed to process log entry: {}", ADMIN_GROUP, e, e1);
                }
            }
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
        RaftGroup config = raftGroupMap.get(ADMIN_GROUP);
        if( config == null) {
            throw new RuntimeException("Node is not a member of configuration group");
        }
        if(! config.localNode().isLeader()) {
            throw new RuntimeException("Node is not leader for configuration, leader = " + config.localNode().getLeader());
        }

        List<Node> raftNodes = contextController.getNodes(nodes);
        String target = nodes.get(0);

        ContextConfiguration contextConfiguration = ContextConfiguration.newBuilder()
                                                                        .setContext(context)
                                                                        .addAllNodes(contextController.getNodeInfos(nodes))
                                                                        .build();
        config.localNode().appendEntry(ContextConfiguration.class.getName(), contextConfiguration.toByteArray());

        if( ! messagingPlatformConfiguration.getName().equals(target)) {
            Context request = Context.newBuilder()
                                     .setName(context)
                                     .addAllMembers(raftNodes.stream().map(r -> ContextMember.newBuilder()
                                                                                   .setHost(r.getHost())
                                                                           .setPort(r.getPort())
                                                                           .setNodeId(r.getNodeId())
                                                                           .build()
                                             ).collect(Collectors.toList()))
                                     .build();
            raftServiceFactory.getRaftGroupService(raftNodes.get(0).getHost(), raftNodes.get(0).getPort()).initContext(
                    request,
                    new StreamObserver<Confirmation>() {
                        @Override
                        public void onNext(Confirmation confirmation) {

                        }

                        @Override
                        public void onError(Throwable throwable) {

                        }

                        @Override
                        public void onCompleted() {

                        }
                    });
        } else {
            RaftGroup raftGroup = initRaftGroup(context);
            RaftNode leader = waitForLeader(raftGroup);
            raftNodes.forEach(n -> {
                try {
                    leader.addNode(n).get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    throw new RuntimeException(e.getCause());
                }
            });
        }
    }

    public void init(List<String> contexts) {
        RaftGroup configGroup = initRaftGroup(ADMIN_GROUP);
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
                                                                        .setContext(ADMIN_GROUP)
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
        RaftGroup config = raftGroupMap.get(ADMIN_GROUP);
        if( config == null) {
            throw new RuntimeException("Node is not a member of configuration group");
        }
        if(! config.localNode().isLeader()) {
            throw new RuntimeException("Node is not leader for configuration, leader = " + config.localNode().getLeader());
        }

        List<String> contexts = new ArrayList<>(); //TODO: nodeInfo.getContextsList().stream().map(c -> c.getName()).collect(Collectors.toList());
        if( contexts.isEmpty()) {
            contexts = contextController.getContexts().map(c -> c.getName()).collect(Collectors.toList());
        }

        Node node = Node.newBuilder().setNodeId(nodeInfo.getNodeName())
                      .setHost(nodeInfo.getInternalHostName())
                      .setPort(nodeInfo.getGrpcInternalPort())
                      .build();
        contexts.forEach(c -> {
            getLeaderService(c).addNodeToContext(node);

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

    @Override
    public RaftNode raftNode(String groupId) {
        RaftGroup raftGroup = raftGroupMap.get(groupId);
        if(raftGroup != null) return raftGroup.localNode();

        synchronized (raftGroupMap) {
            raftGroup = raftGroupMap.get(groupId);
            if(raftGroup == null) {
                raftGroup = createRaftGroup(groupId, messagingPlatformConfiguration.getName());
            }
        }
        return raftGroup.localNode();
    }


    @Scheduled(fixedDelay = 5000)
    public void syncStore() {
        raftGroupMap.forEach((k,e) -> ((GrpcRaftGroup)e).syncStore());
    }

    public void addNodeToContext(String name, String node) {
        RaftGroup config = raftGroupMap.get(ADMIN_GROUP);
        if( config == null) {
            throw new RuntimeException("Node is not a member of configuration group");
        }
        if(! config.localNode().isLeader()) {
            throw new RuntimeException("Node is not leader for configuration, leader = " + config.localNode().getLeader());
        }

        // TODO: What to do if current node is not member or the raft group-> need to find the leader in some other way
        RaftNode leader = raftGroupMap.get(name).localNode();
        String target = leader.getLeader();
        if( ! messagingPlatformConfiguration.getName().equals(target)) {

            Context request = Context.newBuilder().setName(name).addMembers(ContextMember.newBuilder().setNodeId(node).build()).build();
            raftServiceFactory.getRaftGroupService(target).addNodeToContext(request,
                                                                            new StreamObserver<Confirmation>() {
                        @Override
                        public void onNext(Confirmation confirmation) {

                        }

                        @Override
                        public void onError(Throwable throwable) {

                        }

                        @Override
                        public void onCompleted() {

                        }
                    });
        } else {
                try {
                    leader.addNode(Node.newBuilder().setNodeId(node).build()).get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    throw new RuntimeException(e.getCause());
                }
        }
    }


    public void deleteNodeFromContext(String name, String node) {
    }
}
