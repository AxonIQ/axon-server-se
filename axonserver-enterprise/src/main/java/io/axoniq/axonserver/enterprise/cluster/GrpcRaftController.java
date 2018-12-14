package io.axoniq.axonserver.enterprise.cluster;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.LifecycleController;
import io.axoniq.axonserver.cluster.LeaderState;
import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.cluster.StateChanged;
import io.axoniq.axonserver.cluster.grpc.RaftGroupManager;
import io.axoniq.axonserver.cluster.jpa.JpaRaftGroupNode;
import io.axoniq.axonserver.cluster.jpa.JpaRaftGroupNodeRepository;
import io.axoniq.axonserver.cluster.jpa.JpaRaftStateRepository;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.cluster.internal.ReplicationServerStarted;
import io.axoniq.axonserver.enterprise.context.ContextController;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.grpc.cluster.Config;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.internal.Context;
import io.axoniq.axonserver.grpc.internal.ContextConfiguration;
import io.axoniq.axonserver.grpc.internal.ContextMember;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.grpc.internal.State;
import io.axoniq.axonserver.grpc.internal.TransactionWithToken;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.localstorage.TransactionInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    private final LocalRaftConfigService localRaftConfigService;
    private LocalEventStore localEventStore;
    private final Map<String,RaftGroup> raftGroupMap = new ConcurrentHashMap<>();
    private boolean running;
    private volatile boolean replicationServerStarted;
    private final ContextController contextController;
    private final JpaRaftGroupNodeRepository raftGroupNodeRepository;
    private final ApplicationEventPublisher eventPublisher;
    private final LifecycleController lifecycleController;
    private final LocalRaftGroupService localRaftGroupService;
    private ApplicationContext applicationContext;
    private final Map<String, String> leaderMap = new ConcurrentHashMap<>();

    public GrpcRaftController(JpaRaftStateRepository raftStateRepository,
                              RaftServiceFactory raftServiceFactory,
                              MessagingPlatformConfiguration messagingPlatformConfiguration,
                              ContextController contextController,
                              JpaRaftGroupNodeRepository raftGroupNodeRepository,
                              ApplicationEventPublisher eventPublisher,
                              LifecycleController lifecycleController) {
        this.raftStateRepository = raftStateRepository;
        this.raftServiceFactory = raftServiceFactory;
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
        this.contextController = contextController;
        this.raftGroupNodeRepository = raftGroupNodeRepository;
        this.eventPublisher = eventPublisher;
        this.lifecycleController = lifecycleController;
        this.localRaftGroupService = new LocalRaftGroupService();
        this.localRaftConfigService = new LocalRaftConfigService();
    }


    public void start() {
        localEventStore = applicationContext.getBean(LocalEventStore.class);
        raftServiceFactory.setLocalRaftGroupService(localRaftGroupService);
        raftServiceFactory.setLocalRaftConfigService(localRaftConfigService);
        raftServiceFactory.setContextLeaderProvider(this::getStorageMaster);
        Set<JpaRaftGroupNode> groups = raftGroupNodeRepository.findByNodeId(messagingPlatformConfiguration.getName());
        groups.forEach(context -> {
            try {
                createRaftGroup(context.getGroupId(), messagingPlatformConfiguration.getName());
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        });
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

    @EventListener
    public void on(ClusterEvents.MasterConfirmation masterConfirmation) {
        leaderMap.put(masterConfirmation.getContext(), masterConfirmation.getNode());
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

    private RaftGroup createRaftGroup(String groupId, String nodeId) {
        Set<JpaRaftGroupNode> nodes= raftGroupNodeRepository.findByGroupId(groupId);
        RaftGroup raftGroup = new GrpcRaftGroup(nodeId, nodes, groupId, raftStateRepository);

        if( ADMIN_GROUP.equals(groupId)) {
            raftGroup.localNode().registerEntryConsumer(this::processConfigEntry);
        } else {
            raftGroup.localNode().registerEntryConsumer(entry -> processEventEntry(groupId, entry));
            localEventStore.initContext(groupId, false);
        }
        raftGroup.localNode().registerEntryConsumer(entry -> processNewConfiguration(groupId, entry));
        raftGroup.localNode().registerStateChangeListener(stateChanged -> stateChanged(raftGroup.localNode(), stateChanged));

        raftGroupMap.put(groupId, raftGroup);
        if( replicationServerStarted) {
            raftGroup.localNode().start();
        }
        return raftGroup;
    }

    private void stateChanged(RaftNode node, StateChanged stateChanged) {
        if( stateChanged.getFrom().equals(LeaderState.class.getSimpleName())
                && !stateChanged.getTo().equals(LeaderState.class.getSimpleName())) {
            eventPublisher.publishEvent(new ClusterEvents.MasterStepDown(stateChanged.getGroupId(), false));
        }
        if( stateChanged.getTo().equals(LeaderState.class.getSimpleName())
                && !stateChanged.getFrom().equals(LeaderState.class.getSimpleName())) {
            eventPublisher.publishEvent(new ClusterEvents.BecomeMaster(stateChanged.getGroupId(), () -> node.unappliedEntries()));
        }
    }

    @EventListener
    public void on(ReplicationServerStarted replicationServerStarted) {
        this.replicationServerStarted = true;
        raftGroupMap.forEach((k,raftGroup) -> {
            raftGroup.localNode().start();
        });
    }

    private void processEventEntry(String groupId, Entry e) {

            if (e.hasSerializedObject()) {
                logger.debug("{}: received type: {}", groupId, e.getSerializedObject().getType());
                if (e.getSerializedObject().getType().equals("Append.EVENT")) {
                    TransactionWithToken transactionWithToken = null;
                    try {
                        transactionWithToken = TransactionWithToken.parseFrom(e.getSerializedObject().getData());
                        if( logger.isTraceEnabled()) {
                            logger.warn("Index {}: Received Event with index: {} and {} events",
                                         e.getIndex(),
                                         transactionWithToken.getIndex(),
                                         transactionWithToken.getEventsCount()
                            );
                        }
                        if (transactionWithToken.getIndex() > localEventStore.getLastEventIndex(groupId)) {
                            localEventStore.syncEvents(groupId, new TransactionInformation(transactionWithToken.getIndex()), transactionWithToken);
                        } else {
                                logger.warn("Index {}: event already applied",
                                             e.getIndex());
                        }
                    } catch (InvalidProtocolBufferException e1) {
                        throw new RuntimeException("Error processing entry: " + e.getIndex(), e1);
                    }
                } else if (e.getSerializedObject().getType().equals("Append.SNAPSHOT")) {
                    TransactionWithToken transactionWithToken = null;
                    try {
                        transactionWithToken = TransactionWithToken.parseFrom(e.getSerializedObject().getData());
                        if (transactionWithToken.getIndex() > localEventStore.getLastSnapshotIndex(groupId)) {
                            localEventStore.syncSnapshots(groupId, new TransactionInformation(transactionWithToken.getIndex()), transactionWithToken);
                        } else {
                            logger.warn("Index {}: snapshot already applied",
                                        e.getIndex());
                        }
                    } catch (InvalidProtocolBufferException e1) {
                        throw new RuntimeException("Error processing entry: " + e.getIndex(), e1);
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

        return leaderMap.get(s);
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


    public RaftGroupService localRaftGroupService() {
        return localRaftGroupService;
    }

    public RaftConfigService localRaftConfigService() {
        return localRaftConfigService;
    }

    public Iterable<String> getMyStorageContexts() {
        return raftGroupMap.keySet().stream().filter(groupId -> !groupId.equals(ADMIN_GROUP)).collect(Collectors.toList());
    }

    private class LocalRaftGroupService implements RaftGroupService {

        @Override
        public CompletableFuture<Void> addNodeToContext(String name, Node node) {
            RaftNode raftNode = raftGroupMap.get(name).localNode();
            return raftNode.addNode(node);
        }

        @Override
        public void getStatus(Consumer<Context> contextConsumer) {
            raftGroupMap.keySet().forEach(name -> contextConsumer.accept(getStatus(name)));
        }

        @Override
        public CompletableFuture<Void> deleteNode(String context, String node) {
            RaftNode raftNode = raftGroupMap.get(context).localNode();
            return raftNode.removeNode(node);
        }

        @Override
        public CompletableFuture<Void> initContext(String context, List<Node> raftNodes) {
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

            return CompletableFuture.completedFuture(null);
        }

        public Context getStatus(String name) {
            RaftGroup raftGroup = raftGroupMap.get(name);
            String leader = raftGroup.localNode().getLeader();
            return Context.newBuilder().setName(raftGroup.localNode().groupId())
                    .addAllMembers(raftGroup.raftConfiguration()
                                    .groupMembers()
                                    .stream()
                                    .map(n -> ContextMember.newBuilder()
                                                           .setNodeId(n.getNodeId())
                                                           .setState(n.getNodeId().equals(leader) ? State.LEADER : State.VOTING)
                                                           .build())
                                    .collect(Collectors.toList())).build();



        }

        @Override
        public void stepdown(String name) {
            RaftGroup raftGroup = raftGroupMap.get(name);
            if( raftGroup != null && raftGroup.localNode().isLeader()) {
                raftGroup.localNode().stepdown();
            }
        }

    }

    private class LocalRaftConfigService implements RaftConfigService {

        @Override
        public void addNodeToContext(String context, String node) {
            RaftGroup config = raftGroupMap.get(ADMIN_GROUP);

            ClusterNode clusterNode = contextController.getNode(node);
            raftServiceFactory.getRaftGroupService(context).addNodeToContext(context, clusterNode.toNode()).thenApply(r -> {
                ContextConfiguration contextConfiguration = ContextConfiguration.newBuilder()
                                                                                .setContext(context)
                                                                                .addAllNodes(Stream.concat(nodes(context), Stream.of(clusterNode.toNodeInfo()))
                                                                                                   .collect(Collectors.toList())).build();
                        config.localNode().appendEntry(ContextConfiguration.class.getName(), contextConfiguration.toByteArray());
                        return r;
            });
        }

        private Stream<NodeInfo> nodes(String context) {
            return contextController.getContext(context).getMessagingNodes().stream().map(ClusterNode::toNodeInfo);
        }

        @Override
        public void deleteContext(String context) {

        }


        @Override
        public void deleteNodeFromContext(String context, String node) {
            RaftGroup config = raftGroupMap.get(ADMIN_GROUP);

            ClusterNode clusterNode = contextController.getNode(node);
            raftServiceFactory.getRaftGroupService(context).deleteNode(context, node).thenApply(r -> {
                ContextConfiguration contextConfiguration = ContextConfiguration.newBuilder()
                                                                                .setContext(context)
                                                                                .addAllNodes(Stream.of(clusterNode.toNodeInfo())
                                                                                                   .filter(n -> !n.getNodeName().equals(context))
                                                                                                   .collect(Collectors.toList())).build();
                config.localNode().appendEntry(ContextConfiguration.class.getName(), contextConfiguration.toByteArray());
                return r;
            });
        }

        @Override
        public void addContext(String context, List<String> nodes)  {
            RaftGroup config = raftGroupMap.get(ADMIN_GROUP);
            List<Node> raftNodes = contextController.getNodes(nodes);
            Node target = raftNodes.get(0);

            raftServiceFactory.getRaftGroupServiceForNode(target.getNodeId()).initContext(context, raftNodes)
                              .thenApply(r -> {
                                  ContextConfiguration contextConfiguration = ContextConfiguration.newBuilder()
                                                                                                  .setContext(context)
                                                                                                  .addAllNodes(contextController.getNodeInfos(nodes))
                                                                                                  .build();
                                  config.localNode().appendEntry(ContextConfiguration.class.getName(), contextConfiguration.toByteArray());
                                  return r;
                              });
        }

        @Override
        public void join(NodeInfo nodeInfo) {
            RaftGroup config = raftGroupMap.get(ADMIN_GROUP);
            List<String> contexts = new ArrayList<>(); //TODO: nodeInfo.getContextsList().stream().map(c -> c.getName()).collect(Collectors.toList());
            if( contexts.isEmpty()) {
                contexts = contextController.getContexts().map(c -> c.getName()).collect(Collectors.toList());
            }

            Node node = Node.newBuilder().setNodeId(nodeInfo.getNodeName())
                            .setHost(nodeInfo.getInternalHostName())
                            .setPort(nodeInfo.getGrpcInternalPort())
                            .build();
            contexts.forEach(c -> {
                raftServiceFactory.getRaftGroupService(c).addNodeToContext(c, node);

                config.localNode().appendEntry(ContextConfiguration.class.getName(), createContextConfigBuilder(c).addNodes(nodeInfo).build().toByteArray());
            });
        }

        @Override
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
    }
}
