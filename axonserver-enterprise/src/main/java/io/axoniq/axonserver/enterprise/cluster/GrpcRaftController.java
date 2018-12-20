package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.cluster.LeaderState;
import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.cluster.StateChanged;
import io.axoniq.axonserver.cluster.grpc.RaftGroupManager;
import io.axoniq.axonserver.cluster.jpa.JpaRaftGroupNode;
import io.axoniq.axonserver.cluster.jpa.JpaRaftGroupNodeRepository;
import io.axoniq.axonserver.cluster.jpa.JpaRaftStateRepository;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.ContextEvents;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.cluster.internal.ReplicationServerStarted;
import io.axoniq.axonserver.enterprise.config.RaftProperties;
import io.axoniq.axonserver.enterprise.logconsumer.LogEntryConsumer;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;

/**
 * Author: marc
 */
@Controller
public class GrpcRaftController implements SmartLifecycle, ApplicationContextAware, RaftGroupManager {

    private final Logger logger = LoggerFactory.getLogger(GrpcRaftController.class);
    public static final String ADMIN_GROUP = "_admin";
    private final JpaRaftStateRepository raftStateRepository;
    private final MessagingPlatformConfiguration messagingPlatformConfiguration;
    private final Map<String,RaftGroup> raftGroupMap = new ConcurrentHashMap<>();
    private boolean running;
    private volatile boolean replicationServerStarted;
    private final RaftGroupRepositoryManager raftGroupNodeRepository;
    private final RaftProperties raftProperties;
    private final ApplicationEventPublisher eventPublisher;
    private ApplicationContext applicationContext;
    private final JpaRaftGroupNodeRepository nodeRepository;

    public GrpcRaftController(JpaRaftStateRepository raftStateRepository,
                              MessagingPlatformConfiguration messagingPlatformConfiguration,
                              RaftGroupRepositoryManager raftGroupNodeRepository,
                              RaftProperties raftProperties,
                              ApplicationEventPublisher eventPublisher,
                              JpaRaftGroupNodeRepository nodeRepository) {
        this.raftStateRepository = raftStateRepository;
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
        this.raftGroupNodeRepository = raftGroupNodeRepository;
        this.raftProperties = raftProperties;
        this.eventPublisher = eventPublisher;
        this.nodeRepository = nodeRepository;
    }


    public void start() {
        Set<String> groups = raftGroupNodeRepository.getMyContexts();
        groups.forEach(context -> {
            try {
                createRaftGroup(context, messagingPlatformConfiguration.getName());
            } catch (Exception ex) {
                logger.warn("{}: Failed to initialize context", context, ex);
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


    public RaftGroup initRaftGroup(String groupId) {
        Node node = Node.newBuilder()
                        .setNodeId(messagingPlatformConfiguration.getName())
                        .setHost(messagingPlatformConfiguration.getFullyQualifiedInternalHostname())
                        .setPort(messagingPlatformConfiguration.getPort())
                        .build();
        RaftGroup raftGroup = createRaftGroup(groupId, node.getNodeId());
        raftGroup.raftConfiguration().update(singletonList(node));
        return raftGroup;
    }

    private RaftGroup createRaftGroup(String groupId, String localNodeId) {
        RaftGroup raftGroup = new GrpcRaftGroup(localNodeId, raftGroupNodeRepository, groupId, raftStateRepository, raftProperties);

        if( ! ADMIN_GROUP.equals(groupId)) {
            eventPublisher.publishEvent(new ContextEvents.ContextCreated(groupId));
        }
        applicationContext.getBeansOfType(LogEntryConsumer.class).forEach((name, bean) -> raftGroup.localNode().registerEntryConsumer(e -> bean.consumeLogEntry(groupId, e)));
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
            eventPublisher.publishEvent(new ClusterEvents.LeaderStepDown(stateChanged.getGroupId(), false));
        }
        if( stateChanged.getTo().equals(LeaderState.class.getSimpleName())
                && !stateChanged.getFrom().equals(LeaderState.class.getSimpleName())) {
            eventPublisher.publishEvent(new ClusterEvents.BecomeLeader(stateChanged.getGroupId(),
                                                                       node::unappliedEntries));
        }
    }

    @EventListener
    public void on(ReplicationServerStarted replicationServerStarted) {
        this.replicationServerStarted = true;
        raftGroupMap.forEach((k,raftGroup) -> raftGroup.localNode().start());
    }

    public Collection<String> getContexts() {
        return raftGroupMap.keySet();
    }

    public RaftNode getRaftNode(String context) {
        if( ! raftGroupMap.containsKey(context)) {
            throw new MessagingPlatformException(ErrorCode.CONTEXT_NOT_FOUND, messagingPlatformConfiguration.getName() + ": Not a member of " + context);

        }
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
    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }



    RaftNode waitForLeader(RaftGroup group) {
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


    public Iterable<String> getMyContexts() {
        return raftGroupMap.keySet().stream().filter(groupId -> !groupId.equals(ADMIN_GROUP)).collect(Collectors.toList());
    }

    public Set<String> raftGroups() {
        return raftGroupMap.keySet();
    }

    public RaftGroup getRaftGroup(String groupId) {
        return raftGroupMap.get(groupId);
    }
}
