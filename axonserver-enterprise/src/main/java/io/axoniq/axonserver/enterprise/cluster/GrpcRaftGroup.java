package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.cluster.LogEntryProcessor;
import io.axoniq.axonserver.cluster.RaftConfiguration;
import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.cluster.RaftPeer;
import io.axoniq.axonserver.cluster.configuration.MembersStore;
import io.axoniq.axonserver.cluster.configuration.store.JpaMembersStore;
import io.axoniq.axonserver.cluster.election.ElectionStore;
import io.axoniq.axonserver.cluster.grpc.GrpcRaftClientFactory;
import io.axoniq.axonserver.cluster.grpc.GrpcRaftPeer;
import io.axoniq.axonserver.cluster.jpa.JpaRaftGroupNodeRepository;
import io.axoniq.axonserver.cluster.jpa.JpaRaftStateController;
import io.axoniq.axonserver.cluster.jpa.JpaRaftStateRepository;
import io.axoniq.axonserver.cluster.replication.LogEntryStore;
import io.axoniq.axonserver.cluster.replication.file.DefaultLogEntryTransformerFactory;
import io.axoniq.axonserver.cluster.replication.file.FileSegmentLogEntryStore;
import io.axoniq.axonserver.cluster.replication.file.IndexManager;
import io.axoniq.axonserver.cluster.replication.file.LogEntryTransformerFactory;
import io.axoniq.axonserver.cluster.replication.file.PrimaryLogEntryStore;
import io.axoniq.axonserver.cluster.replication.file.SecondaryLogEntryStore;
import io.axoniq.axonserver.enterprise.cluster.snapshot.AxonServerSnapshotManager;
import io.axoniq.axonserver.enterprise.cluster.snapshot.SnapshotDataStore;
import io.axoniq.axonserver.enterprise.config.RaftProperties;
import io.axoniq.axonserver.grpc.cluster.Node;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * @author Marc Gathier
 */
public class GrpcRaftGroup implements RaftGroup {
    private final LogEntryStore localLogEntryStore;
    private final JpaRaftStateController raftStateController;
    private final RaftConfiguration raftConfiguration;
    private final RaftNode localNode;
    private final LogEntryProcessor logEntryProcessor;
    private final GrpcRaftClientFactory clientFactory;

    public GrpcRaftGroup(String localNodeId, String groupId,
                         JpaRaftStateRepository raftStateRepository, JpaRaftGroupNodeRepository nodeRepository,
                         RaftProperties storageOptions,
                         Function<String, List<SnapshotDataStore>> snapshotDataProvidersFactory,
                         GrpcRaftClientFactory clientFactory) {
        this.clientFactory = clientFactory;
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

        localLogEntryStore = new FileSegmentLogEntryStore(groupId, primary);

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
                membersStore.set(nodes);
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
        };

        List<SnapshotDataStore> dataProviders = snapshotDataProvidersFactory.apply(groupId);
        localNode = new RaftNode(localNodeId, this, new AxonServerSnapshotManager(dataProviders));

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
    public RaftPeer peer(String nodeId) {
        List<Node> nodes = raftConfiguration.groupMembers();
        for (Node node : nodes) {
            if (node.getNodeId().equals(nodeId)){
                return new GrpcRaftPeer(node, clientFactory, raftConfiguration.maxElectionTimeout());
            }
        }
        throw new IllegalArgumentException(nodeId + " is not member of this group");
    }

    @Override
    public RaftPeer peer(Node node) {
        return new GrpcRaftPeer(node, clientFactory, raftConfiguration.maxElectionTimeout());
    }

    @Override
    public RaftNode localNode() {
        return localNode;
    }

    public void syncStore() {
        raftStateController.sync();
    }
}
