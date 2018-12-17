package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.cluster.LogEntryProcessor;
import io.axoniq.axonserver.cluster.RaftConfiguration;
import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.cluster.RaftPeer;
import io.axoniq.axonserver.cluster.election.ElectionStore;
import io.axoniq.axonserver.cluster.grpc.GrpcRaftPeer;
import io.axoniq.axonserver.cluster.jpa.JpaRaftGroupNode;
import io.axoniq.axonserver.cluster.jpa.JpaRaftStateController;
import io.axoniq.axonserver.cluster.jpa.JpaRaftStateRepository;
import io.axoniq.axonserver.cluster.replication.LogEntryStore;
import io.axoniq.axonserver.cluster.replication.file.DefaultLogEntryTransformerFactory;
import io.axoniq.axonserver.cluster.replication.file.LogEntryTransformerFactory;
import io.axoniq.axonserver.cluster.replication.file.FileSegmentLogEntryStore;
import io.axoniq.axonserver.cluster.replication.file.IndexManager;
import io.axoniq.axonserver.cluster.replication.file.PrimaryLogEntryStore;
import io.axoniq.axonserver.cluster.replication.file.SecondaryLogEntryStore;
import io.axoniq.axonserver.cluster.replication.file.StorageProperties;
import io.axoniq.axonserver.grpc.cluster.Node;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * Author: marc
 */
public class GrpcRaftGroup implements RaftGroup {
    private final LogEntryStore localLogEntryStore;
    private final JpaRaftStateController raftStateController;
    private final RaftConfiguration raftConfiguration;
    private final RaftNode localNode;
    private final LogEntryProcessor logEntryProcessor;
    private final Map<String, RaftPeer> peers  = new ConcurrentHashMap<>();

    public GrpcRaftGroup(String nodeId, Set<JpaRaftGroupNode> nodes, String groupId,
                         JpaRaftStateRepository raftStateRepository) {
        LogEntryTransformerFactory eventTransformerFactory = new DefaultLogEntryTransformerFactory();
        StorageProperties storageOptions = new StorageProperties();
        storageOptions.setSegmentSize(16*1024*1024);
        storageOptions.setLogStorageFolder("log");


        IndexManager indexManager = new IndexManager(storageOptions, groupId);
        PrimaryLogEntryStore primary = new PrimaryLogEntryStore(groupId,
                                                                indexManager,
                                                                eventTransformerFactory,
                                                                storageOptions);
        primary.setNext(new SecondaryLogEntryStore(groupId, indexManager, eventTransformerFactory, storageOptions));
        primary.initSegments(Long.MAX_VALUE);

        localLogEntryStore = new FileSegmentLogEntryStore(groupId, primary);
        raftStateController = new JpaRaftStateController(groupId, raftStateRepository);
        raftConfiguration = new RaftConfiguration() {
            @Override
            public List<Node> groupMembers() {
                return peers.values()
                            .stream()
                            .map(p -> p.toNode())
                            .collect(Collectors.toList());
            }

            @Override
            public String groupId() {
                return groupId;
            }

            @Override
            public void update(List<Node> nodes) {

            }

            @Override
            public int minElectionTimeout() {
                return 1000;
            }

            @Override
            public int maxElectionTimeout() {
                return 2500;
            }

            @Override
            public int heartbeatTimeout() {
                return 250;
            }
        };

        nodes.forEach(node -> {
            GrpcRaftPeer raftPeer = new GrpcRaftPeer(Node.newBuilder()
                                                             .setNodeId(node.getNodeId())
                                                             .setPort(node.getPort())
                                                             .setHost(node.getHost())
                                                             .build());
            peers.put(node.getNodeId(), raftPeer);
        });

        localNode = new RaftNode(nodeId, this);
        logEntryProcessor = new LogEntryProcessor(raftStateController);
        raftStateController.init();

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
        return peers.get(nodeId);
    }

    @Override
    public RaftPeer peer(Node node) {
        return new GrpcRaftPeer(node);
    }

    @Override
    public RaftNode localNode() {
        return localNode;
    }

    @Override
    public void registerNode(Node node) {
        if( ! peers.containsKey(node.getNodeId())) {
            peers.put(node.getNodeId(), new GrpcRaftPeer(node));
        }
    }

    @Override
    public void unregisterNode(String nodeID) {
        peers.remove(nodeID);
    }

    public void syncStore() {
        raftStateController.sync();
    }
}
