package io.axoniq.axonserver.cluster.grpc;

import io.axoniq.axonserver.cluster.InMemoryProcessorStore;
import io.axoniq.axonserver.cluster.LogEntryProcessor;
import io.axoniq.axonserver.cluster.RaftConfiguration;
import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.cluster.RaftPeer;
import io.axoniq.axonserver.cluster.RaftServerConfiguration;
import io.axoniq.axonserver.cluster.election.ElectionStore;
import io.axoniq.axonserver.cluster.election.InMemoryElectionStore;
import io.axoniq.axonserver.cluster.replication.InMemoryLogEntryStore;
import io.axoniq.axonserver.cluster.replication.LogEntryStore;
import io.axoniq.axonserver.cluster.snapshot.FakeSnapshotManager;
import io.axoniq.axonserver.grpc.cluster.Node;
import org.junit.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import static io.axoniq.axonserver.cluster.TestUtils.assertWithin;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static junit.framework.TestCase.*;

/**
 * @author Sara Pellegrini
 * @since 4.1
 */
public class InstallSnapshotIntegrationTest {

    private Map<String, RaftServer> raftServers;
    private Map<String, RaftNode> clusterNodes;
    private RaftNode leader;
    private List<String> followers;

    private Node node(String id, int port) {
        return Node.newBuilder().setNodeId(id).setHost("localhost").setPort(port).build();
    }

    @Before
    public void setUp() throws Exception {
        List<Node> nodes = asList(node("node1", 7777), node("node2", 7778), node("node3", 7779));
        raftServers = new HashMap<>();
        clusterNodes = new HashMap<>();
        IntStream.range(0, nodes.size()).forEach(i -> {
            Node node = nodes.get(i);
            GrpcRaftGroup raftGroup = new GrpcRaftGroup(nodes, node.getNodeId(), 100 + i*1000, 1000 + i*1000);
            RaftNode raftNode = raftGroup.localNode();
            clusterNodes.put(node.getNodeId(), raftNode);
            FakeRaftGroupManager raftGroupManager = new FakeRaftGroupManager(raftNode);
            LeaderElectionService leaderElectionService = new LeaderElectionService(raftGroupManager);
            LogReplicationService logReplicationService = new LogReplicationService(raftGroupManager);
            RaftServerConfiguration configuration = new RaftServerConfiguration();
            configuration.setInternalPort(node.getPort());
            RaftServer raftServer = new RaftServer(configuration, leaderElectionService, logReplicationService);
            raftServers.put(node.getNodeId(), raftServer);
        });

        raftServers.forEach((id, raftServer) -> raftServer.start());
        clusterNodes.forEach((id, node) -> node.start());
        assertWithin(15, TimeUnit.SECONDS, () -> {
            RaftNode leader = clusterNodes.values().stream().filter(RaftNode::isLeader).findFirst().orElse(null);
            assertNotNull(leader);
        });
        leader = clusterNodes.values().stream().filter(RaftNode::isLeader).findFirst().orElse(null);
        String leaderId = leader.nodeId();
        followers = raftServers.keySet().stream().filter(id -> !id.equals(leaderId)).collect(toList());
    }

    @Test
    @Ignore("This test is not stable on Jenkins, ignoring for now")
    public void start() throws InterruptedException, ExecutionException, TimeoutException {
        RaftNode firstFollower = clusterNodes.get(followers.get(0));
        RaftNode secondFollower = clusterNodes.get(followers.get(1));

        assertEquals(2, followers.size());
        firstFollower.stop();
        int entryPerRound = 1000;
        CompletableFuture[] futures = new CompletableFuture[entryPerRound];
        for (int i = 0; i < futures.length; i++) {
            futures[i] = leader.appendEntry("Test-A-" + i, ("Test-A-" + i).getBytes());
        }
        CompletableFuture<Void> firstFuture = CompletableFuture.allOf(futures);
        firstFuture.get(25, TimeUnit.SECONDS);
        assertTrue(leader.isLeader());
        assertWithin(15, TimeUnit.SECONDS, commitIndexSynchronized(secondFollower, leader));
        assertNotNull(leader.raftGroup().localLogEntryStore().getEntry(2));
        leader.forceLogCleaning(0, MINUTES);
        assertTrue(leader.isLeader());
        assertNull(leader.raftGroup().localLogEntryStore().getEntry(2));
        firstFollower.start();
        assertWithin(10, TimeUnit.SECONDS, commitIndexSynchronized(firstFollower, leader));
        assertTrue(leader.isLeader());
        secondFollower.stop();
        assertFalse(firstFollower.isLeader());
        assertTrue(leader.isLeader());

        for (int i = 0; i < futures.length; i++) {
            futures[i] = leader.appendEntry("Test-B-" + i, ("Test-B-" + i).getBytes());
        }
        CompletableFuture<Void> secondFuture = CompletableFuture.allOf(futures);
        secondFuture.get(15, TimeUnit.SECONDS);
        assertWithin(15, TimeUnit.SECONDS, commitIndexSynchronized(firstFollower, leader));
        assertNotNull(leader.raftGroup().localLogEntryStore().getEntry(entryPerRound * 2L - 3L));
        leader.forceLogCleaning(0, MINUTES);
        assertNull(leader.raftGroup().localLogEntryStore().getEntry(entryPerRound * 2L - 3L));
        assertTrue(leader.isLeader());

        assertWithin(15, TimeUnit.SECONDS,
                     () -> assertTrue(secondFollower.raftGroup().logEntryProcessor().commitIndex() <
                                              leader.raftGroup().logEntryProcessor().commitIndex()));
        secondFollower.start();
        assertWithin(15, TimeUnit.SECONDS, commitIndexSynchronized(firstFollower, leader));
        assertWithin(15, TimeUnit.SECONDS, commitIndexSynchronized(secondFollower, leader));
    }

    private Runnable commitIndexSynchronized(RaftNode node, RaftNode other) {
        return () -> assertEquals(node.raftGroup().logEntryProcessor().commitIndex(),
                                  other.raftGroup().logEntryProcessor().commitIndex());
    }

    @After
    public void tearDown() {
        leader.stop();
        clusterNodes.get(followers.get(0)).stop();
        clusterNodes.get(followers.get(1)).stop();
        raftServers.forEach((id, server) -> server.stop());
    }

    private static class GrpcRaftGroup implements RaftGroup, RaftConfiguration {

        final LogEntryStore logEntryStore;
        private final int minElectionTimeout;
        private final int maxElectionTimeout;
        final ElectionStore electionStore;
        final LogEntryProcessor logEntryProcessor;
        final RaftNode localNode;
        final Map<String, RaftPeer> raftPeerMap = new HashMap<>();
        final List<Node> nodes;

        private GrpcRaftGroup(List<Node> nodes, String localNode, int minElectionTimeout, int maxElectionTimeout) {
            this.nodes = nodes;
            logEntryStore = new InMemoryLogEntryStore(localNode);
            this.minElectionTimeout = minElectionTimeout;
            this.maxElectionTimeout = maxElectionTimeout;
            logEntryProcessor = new LogEntryProcessor(new InMemoryProcessorStore());
            electionStore = new InMemoryElectionStore();
            this.localNode = new RaftNode(localNode, this, new FakeSnapshotManager());
            initializePeers(nodes);
        }

        private void initializePeers(List<Node> nodes) {
            raftPeerMap.clear();
            nodes.forEach(node -> raftPeerMap.put(node.getNodeId(), new GrpcRaftPeer(groupId(), node)));
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
        public LogEntryProcessor logEntryProcessor() {
            return logEntryProcessor;
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
        public RaftPeer peer(Node node) {
            return new GrpcRaftPeer(groupId(), node);
        }

        @Override
        public RaftNode localNode() {
            return localNode;
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
        public void update(List<Node> nodes) {
            initializePeers(nodes);
        }

        @Override
        public int minElectionTimeout() {
            return minElectionTimeout;
        }

        @Override
        public int maxElectionTimeout() {
            return maxElectionTimeout;
        }

        @Override
        public void delete() {

        }
    }
}
