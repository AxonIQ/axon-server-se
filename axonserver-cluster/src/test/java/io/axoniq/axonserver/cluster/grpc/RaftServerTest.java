package io.axoniq.axonserver.cluster.grpc;

import io.axoniq.axonserver.cluster.RaftConfiguration;
import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.cluster.RaftPeer;
import io.axoniq.axonserver.cluster.RaftServerConfiguration;
import io.axoniq.axonserver.cluster.election.ElectionStore;
import io.axoniq.axonserver.cluster.election.InMemoryElectionStore;
import io.axoniq.axonserver.cluster.replication.InMemoryLogEntryStore;
import io.axoniq.axonserver.cluster.replication.LogEntryStore;
import io.axoniq.axonserver.grpc.cluster.Node;
import org.junit.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 * Author: marc
 */
public class RaftServerTest {

    Map<String, RaftNode> clusterNodes = new HashMap<>();
    List<Node> nodes = Arrays.asList(node("node1", "localhost", 7777),
                                     node("node2", "localhost", 7778),
//                                     node("node3", "localhost", 7779),
//                                     node("node4", "localhost", 7780),
                                     node("node5", "localhost", 7781));

    List<RaftServer> raftServers = new ArrayList<>();


    @Before
    public void setUp() throws Exception {

        nodes.forEach(node -> {
            RaftGroup raftGroup = new GrpcRaftGroup(nodes, node.getNodeId());
            clusterNodes.put(node.getNodeId(), new RaftNode(node.getNodeId(), raftGroup));
        });

        nodes.forEach(node -> {
            RaftNode raftNode = clusterNodes.get(node.getNodeId());
            LeaderElectionService leaderElectionService = new LeaderElectionService();
            leaderElectionService.addRaftNode(raftNode);

            LogReplicationService logReplicationService = new LogReplicationService();
            logReplicationService.addRaftNode(raftNode);


            RaftServerConfiguration configuration = new RaftServerConfiguration();
            configuration.setInternalPort(node.getPort());
            RaftServer raftServer = new RaftServer(configuration, leaderElectionService, logReplicationService);
            raftServers.add(raftServer);
        });


    }

    private Node node(String id, String hostname, int port) {
        return Node.newBuilder().setNodeId(id).setHost(hostname).setPort(port).build();
    }

    @Test
    public void start() throws InterruptedException, TimeoutException, ExecutionException {
        raftServers.forEach(r -> r.start());
        clusterNodes.forEach((id, node) -> node.start());

        try {

            RaftNode leader = null;
            while( leader == null) {
                leader = clusterNodes.values().stream().filter(n -> n.isLeader()).findFirst().orElse(null);
                Thread.sleep(5);
            }

            CompletableFuture<Void>[] futures = new CompletableFuture[50];
            AtomicInteger successCount = new AtomicInteger();
            final RaftNode fleader  = leader;
            long before = System.currentTimeMillis();
            IntStream.range(0, futures.length).forEach(i -> futures[i] = fleader.appendEntry("Test", "Test".getBytes()).thenAccept(v-> {
                successCount.incrementAndGet();
            }));
            CompletableFuture.allOf(futures).get(25, TimeUnit.SECONDS);
            long after = System.currentTimeMillis();
            Thread.sleep(TimeUnit.SECONDS.toMillis(3));
            System.out.println( "Applied on leader within " + (after-before) + "ms.");
        } finally {
            clusterNodes.forEach((id, node) -> node.stop());
            raftServers.forEach(r -> r.stop(() -> {}));
        }
    }

    private class GrpcRaftGroup implements RaftGroup, RaftConfiguration {

        final LogEntryStore logEntryStore;
        final ElectionStore electionStore;
        final Map<String,RaftPeer> raftPeerMap = new HashMap<>();
        private final String localNode;

        private GrpcRaftGroup(List<Node> nodes, String localNode) {
            this.localNode = localNode;
            logEntryStore = new InMemoryLogEntryStore();
            electionStore = new InMemoryElectionStore();
            nodes.forEach(node -> raftPeerMap.put(node.getNodeId(), new GrpcRaftPeer(node)));
        }


        @Override
        public LogEntryStore localLogEntryStore() {
            return logEntryStore;
        }

        @Override
        public ElectionStore localElectionStore() {
            return electionStore;
        }

        @Override
        public RaftConfiguration raftConfiguration() {
            return this;
        }

        @Override
        public RaftPeer peer(String nodeId) {
            return raftPeerMap.get(nodeId);
        }

        @Override
        public RaftNode localNode() {
            return clusterNodes.get(localNode);
        }

        @Override
        public List<Node> groupMembers() {
            return nodes;
        }

        @Override
        public String groupId() {
            return "MyGroupId";
        }

        @Override
        public long minElectionTimeout() {
            return 1000;
        }

        @Override
        public long maxElectionTimeout() {
            return 3000;
        }

        @Override
        public long heartbeatTimeout() {
            return 100;
        }
    }
}