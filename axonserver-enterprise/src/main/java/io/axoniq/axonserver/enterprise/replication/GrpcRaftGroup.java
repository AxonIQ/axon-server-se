package io.axoniq.axonserver.enterprise.replication;

import io.axoniq.axonserver.cluster.LogEntryProcessor;
import io.axoniq.axonserver.cluster.NewConfigurationConsumer;
import io.axoniq.axonserver.cluster.RaftConfiguration;
import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.cluster.RaftPeer;
import io.axoniq.axonserver.cluster.configuration.MembersStore;
import io.axoniq.axonserver.cluster.configuration.store.JpaMembersStore;
import io.axoniq.axonserver.cluster.election.ElectionStore;
import io.axoniq.axonserver.cluster.grpc.GrpcRaftClientFactory;
import io.axoniq.axonserver.cluster.grpc.GrpcRaftPeer;
import io.axoniq.axonserver.cluster.jpa.JpaRaftStateController;
import io.axoniq.axonserver.cluster.jpa.JpaRaftStateRepository;
import io.axoniq.axonserver.cluster.jpa.ReplicationGroupMemberRepository;
import io.axoniq.axonserver.cluster.replication.LogEntryStore;
import io.axoniq.axonserver.cluster.replication.file.DefaultLogEntryTransformerFactory;
import io.axoniq.axonserver.cluster.replication.file.FileSegmentLogEntryStore;
import io.axoniq.axonserver.cluster.replication.file.IndexManager;
import io.axoniq.axonserver.cluster.replication.file.LogEntryTransformerFactory;
import io.axoniq.axonserver.cluster.replication.file.PrimaryLogEntryStore;
import io.axoniq.axonserver.cluster.replication.file.SecondaryLogEntryStore;
import io.axoniq.axonserver.cluster.scheduler.DefaultScheduler;
import io.axoniq.axonserver.cluster.util.RoleUtils;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.config.RaftProperties;
import io.axoniq.axonserver.enterprise.replication.group.ReplicationGroupController;
import io.axoniq.axonserver.enterprise.replication.logconsumer.EventLogEntryConsumer;
import io.axoniq.axonserver.enterprise.replication.logconsumer.SnapshotLogEntryConsumer;
import io.axoniq.axonserver.enterprise.replication.snapshot.AxonServerSnapshotManager;
import io.axoniq.axonserver.enterprise.replication.snapshot.SnapshotDataStore;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nonnull;

import static io.axoniq.axonserver.RaftAdminGroup.isAdmin;

/**
 * @author Marc Gathier
 */
public class GrpcRaftGroup implements RaftGroup {

    private final String replicationGroup;
    private final LogEntryStore localLogEntryStore;
    private final JpaRaftStateController raftStateController;
    private final RaftConfiguration raftConfiguration;
    private final RaftNode localNode;
    private final LogEntryProcessor logEntryProcessor;
    private final LocalEventStore localEventStore;
    private final GrpcRaftClientFactory clientFactory;
    private final ReplicationGroupController replicationGroupController;

    public GrpcRaftGroup(String localNodeId, String groupId,
                         JpaRaftStateRepository raftStateRepository, ReplicationGroupMemberRepository nodeRepository,
                         RaftProperties storageOptions,
                         Function<String, List<SnapshotDataStore>> snapshotDataProvidersFactory,
                         LocalEventStore localEventStore,
                         GrpcRaftClientFactory clientFactory,
                         MessagingPlatformConfiguration messagingPlatformConfiguration,
                         NewConfigurationConsumer newConfigurationConsumer,
                         PlatformTransactionManager platformTransactionManager,
                         ReplicationGroupController replicationGroupController) {
        this.clientFactory = clientFactory;
        replicationGroup = groupId;
        this.localEventStore = localEventStore;
        this.replicationGroupController = replicationGroupController;
        raftStateController = new JpaRaftStateController(groupId, raftStateRepository);
        raftStateController.init();
        logEntryProcessor = new LogEntryProcessor(raftStateController);
        LogEntryTransformerFactory eventTransformerFactory = new DefaultLogEntryTransformerFactory();
        IndexManager indexManager = new IndexManager(storageOptions, groupId);
        PrimaryLogEntryStore primary = new PrimaryLogEntryStore(groupId,
                                                                indexManager,
                                                                eventTransformerFactory,
                                                                storageOptions);
        primary.setNext(new SecondaryLogEntryStore(groupId, indexManager, eventTransformerFactory, storageOptions));
        primary.initSegments(Long.MAX_VALUE);

        localLogEntryStore = new FileSegmentLogEntryStore(groupId, primary, logEntryProcessor::commitIndex);

        raftConfiguration = new RaftConfiguration() {

            private final MembersStore membersStore = new JpaMembersStore(this::groupId, nodeRepository);

            @Override
            public List<Node> groupMembers() {
                return membersStore.get();
            }

            @Override
            public String groupId() {
                return groupId;
            }

            @Override
            public void update(List<Node> nodes) {
                new TransactionTemplate(platformTransactionManager)
                        .execute(new TransactionCallbackWithoutResult() {
                    @Override
                    protected void doInTransactionWithoutResult(@Nonnull TransactionStatus transactionStatus) {
                        membersStore.set(nodes);
                    }
                });
            }

            @Override
            public void delete() {
                membersStore.set(Collections.emptyList());
            }

            @Override
            public int minElectionTimeout() {
                return storageOptions.getMinElectionTimeout();
            }

            @Override
            public int maxElectionTimeout() {
                return storageOptions.getMaxElectionTimeout();
            }

            @Override
            public int initialElectionTimeout() {
                return storageOptions.getInitialElectionTimeout();
            }

            @Override
            public boolean forceSnapshotOnJoin() {
                return storageOptions.isForceSnapshotOnJoin();
            }

            @Override
            public int heartbeatTimeout() {
                return storageOptions.getHeartbeatTimeout();
            }

            @Override
            public int flowBuffer() {
                return storageOptions.getFlowBuffer();
            }

            @Override
            public int maxEntriesPerBatch() {
                return storageOptions.getMaxEntriesPerBatch();
            }

            @Override
            public boolean isLogCompactionEnabled() {
                return storageOptions.isLogCompactionEnabled();
            }

            @Override
            public int logRetentionHours() {
                return storageOptions.getLogRetentionHours();
            }

            @Override
            public int maxReplicationRound() {
                return storageOptions.getMaxReplicationRound();
            }

            @Override
            public int maxSnapshotNoOfChunksPerBatch() {
                return storageOptions.getMaxSnapshotChunksPerBatch();
            }

            @Override
            public int snapshotFlowBuffer() {
                return storageOptions.getSnapshotFlowBuffer();
            }

            @Override
            public int maxMessageSize() {
                return messagingPlatformConfiguration.getMaxMessageSize();
            }

            @Override
            public boolean isSerializedEventData(String type) {
                return EventLogEntryConsumer.LOG_ENTRY_TYPE.equals(type)
                        || SnapshotLogEntryConsumer.LOG_ENTRY_TYPE.equals(type);
            }

            @Override
            public Integer minActiveBackups() {
                return storageOptions.getMinActiveBackups();
            }
        };

        List<SnapshotDataStore> dataProviders = snapshotDataProvidersFactory.apply(groupId);
        localNode = new RaftNode(localNodeId,
                                 this,
                                 new DefaultScheduler(groupId + "-raftNode"),
                                 new AxonServerSnapshotManager(dataProviders),
                                 newConfigurationConsumer);

    }

    @Override
    public LogEntryStore localLogEntryStore() {
        return localLogEntryStore;
    }

    @Override
    public ElectionStore localElectionStore() {
        return raftStateController;
    }

    @Override
    public RaftConfiguration raftConfiguration() {
        return raftConfiguration;
    }

    @Override
    public LogEntryProcessor logEntryProcessor() {
        return logEntryProcessor;
    }

    @Override
    public RaftPeer peer(Node node) {
        return new GrpcRaftPeer(replicationGroup, node, clientFactory, raftConfiguration.maxElectionTimeout());
    }

    @Override
    public RaftNode localNode() {
        return localNode;
    }

    public void syncStore() {
        raftStateController.sync();
    }

    @Override
    public long lastAppliedEventSequence(String context) {
        try {
            return localEventStore.getLastEvent(context);
        } catch (MessagingPlatformException mpe) {
            if (ErrorCode.NO_EVENTSTORE.equals(mpe.getErrorCode())) {
                return -1;
            }
            throw mpe;
        }
    }


    @Override
    public long lastAppliedSnapshotSequence(String context) {
        try {
            return localEventStore.getLastSnapshot(context);
        } catch (MessagingPlatformException mpe) {
            if (ErrorCode.NO_EVENTSTORE.equals(mpe.getErrorCode())) {
                return -1;
            }
            throw mpe;
        }
    }

    @Override
    public Map<String, Long> lastEventTokenPerContext(Role role) {
        if (isAdmin(replicationGroup) || !RoleUtils.hasStorage(role)) {
            return Collections.emptyMap();
        }
        Map<String, Long> map = new HashMap<>();
        replicationGroupController.getContextNames(replicationGroup)
                                  .forEach(context -> map.put(context, lastAppliedEventSequence(context)));
        return map;
    }

    @Override
    public Map<String, Long> lastSnapshotTokenPerContext(Role role) {
        if (isAdmin(replicationGroup) || !RoleUtils.hasStorage(role)) {
            return Collections.emptyMap();
        }
        Map<String, Long> map = new HashMap<>();
        replicationGroupController.getContextNames(replicationGroup)
                                  .forEach(context -> map.put(context, lastAppliedSnapshotSequence(context)));
        return map;
    }

    @Override
    public boolean leaderSupportsReplicationGroup() {
        return false;
    }
}
