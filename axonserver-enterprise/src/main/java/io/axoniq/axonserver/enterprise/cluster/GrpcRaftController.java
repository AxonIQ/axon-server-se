package io.axoniq.axonserver.enterprise.cluster;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.cluster.grpc.LeaderElectionService;
import io.axoniq.axonserver.cluster.grpc.LogReplicationService;
import io.axoniq.axonserver.cluster.jpa.JpaRaftStateRepository;
import io.axoniq.axonserver.cluster.replication.file.GroupContext;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.jpa.Context;
import io.axoniq.axonserver.grpc.internal.TransactionWithToken;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Controller;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.persistence.EntityManager;

/**
 * Author: marc
 */
@Controller
public class GrpcRaftController implements SmartLifecycle, ApplicationContextAware {

    private final LogReplicationService logReplicationService;
    private final LeaderElectionService leaderElectionService;
    private final JpaRaftStateRepository raftStateRepository;
    private final MessagingPlatformConfiguration messagingPlatformConfiguration;
    private final EntityManager entityManager;
    private LocalEventStore localEventStore;
    private final Map<String,RaftGroup> raftGroupMap = new ConcurrentHashMap<>();
    private boolean running;
    private ApplicationContext applicationContext;

    public GrpcRaftController(LogReplicationService logReplicationService,
                              LeaderElectionService leaderElectionService,
                              JpaRaftStateRepository raftStateRepository,
                              MessagingPlatformConfiguration messagingPlatformConfiguration,
                              EntityManager entityManager

                              ) {
        this.logReplicationService = logReplicationService;
        this.leaderElectionService = leaderElectionService;
        this.raftStateRepository = raftStateRepository;
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
        this.entityManager = entityManager;
    }


    public void start() {
        localEventStore = applicationContext.getBean(LocalEventStore.class);
        ClusterNode clusterNode = entityManager.find(ClusterNode.class, messagingPlatformConfiguration.getName());
        clusterNode.getStorageContexts().forEach(context -> {
                                                     RaftGroup raftGroup = createRaftGroup(context, messagingPlatformConfiguration.getName());
                                                     raftGroupMap.put(context.getName(), raftGroup);

        }
                                                         );
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

    public RaftGroup createRaftGroup(Context myStorageContext, String nodeId) {
        RaftGroup raftGroup = new GrpcRaftGroup(nodeId, myStorageContext.getStorageNodes(), new GroupContext(myStorageContext.getName(), myStorageContext.getName() + ".storage" ), raftStateRepository);
        logReplicationService.addRaftNode(raftGroup.localNode());
        leaderElectionService.addRaftNode(raftGroup.localNode());
        raftGroup.localNode().registerEntryConsumer(e -> {
            System.out.println( e.hasSerializedObject());
            if( e.hasSerializedObject()) {
                System.out.println( e.getSerializedObject().getType());
                if( e.getSerializedObject().getType().equals("Append.EVENT")) {
                    TransactionWithToken transactionWithToken = null;
                    try {
                        transactionWithToken = TransactionWithToken.parseFrom(e.getSerializedObject().getData());
                        System.out.println( transactionWithToken.getToken());
                        if( transactionWithToken.getToken() > localEventStore.getLastToken(myStorageContext.getName())) {
                            localEventStore.syncEvents(myStorageContext.getName(), transactionWithToken);
                        }
                    } catch (InvalidProtocolBufferException e1) {
                        e1.printStackTrace();
                    }
                }
            }
        });

        raftGroup.localNode().start();
        return raftGroup;
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
}
