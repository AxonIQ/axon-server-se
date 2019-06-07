package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.election.ElectionStore;
import io.axoniq.axonserver.cluster.election.InMemoryElectionStore;
import io.axoniq.axonserver.cluster.replication.InMemoryLogEntryStore;
import io.axoniq.axonserver.cluster.replication.LogEntryStore;
import io.axoniq.axonserver.cluster.scheduler.DefaultScheduler;
import io.axoniq.axonserver.cluster.scheduler.Scheduler;
import io.axoniq.axonserver.cluster.snapshot.FakeSnapshotManager;
import io.axoniq.axonserver.cluster.snapshot.SnapshotManager;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

public class RaftClusterTestFixture {

    private static final Logger logger = LoggerFactory.getLogger(RaftClusterTestFixture.class);
    private final ScheduledExecutorService remoteCommunication = new ScheduledThreadPoolExecutor(1, new ThreadPoolExecutor.CallerRunsPolicy());

    private Map<String, StubRaftGroup> clusterGroups = new ConcurrentHashMap<>();
    private Map<String, RaftNode> clusterNodes = new ConcurrentHashMap<>();
    private Map<String, Set<String>> communicationProblems = new ConcurrentHashMap<>();
    private Map<String, Map<String, Supplier<Integer>>> communicationDelay = new ConcurrentHashMap<>();
    private Supplier<Integer> defaultDelay = () -> ThreadLocalRandom.current().nextInt(1, 5);
    private final SnapshotManager snapshotManager;
    private final Scheduler scheduler;

    public RaftClusterTestFixture(String... hostNames) {
        this(new FakeSnapshotManager(), new DefaultScheduler(), hostNames);
    }

    public RaftClusterTestFixture(SnapshotManager snapshotManager, Scheduler scheduler, String... hostNames) {
        this.snapshotManager = snapshotManager;
        this.scheduler = scheduler;
        for (String hostName : hostNames) {
            spawnNew(hostName);
        }
        clusterGroups.forEach((id, group) -> group.update(clusterNodes));
    }

    public RaftGroup getGroup(String name) {
        return clusterGroups.get(name);
    }

    public RaftNode getNode(String name) {
        return clusterNodes.get(name);
    }

    public void startNodes() {
        clusterNodes.values().forEach(RaftNode::start);
    }

    public Set<String> leaders() {
        return clusterNodes.values().stream().filter(RaftNode::isLeader)
                           .map(RaftNode::nodeId).collect(Collectors.toSet());
    }

    public RaftNode spawnNew(String hostName) {
        StubRaftGroup raftGroup = new StubRaftGroup(hostName);
        clusterGroups.put(hostName, raftGroup);
        RaftNode raftNode = new RaftNode(hostName, raftGroup, scheduler, snapshotManager);
        clusterNodes.put(hostName, raftNode);
        return raftNode;
    }

    /**
     * Simulate communication problems for the given {@code host} by blocking all communication from and to
     * it.
     *
     * @param host The host to simulate connection issues for
     */
    public void disconnect(String host) {
        clusterNodes.keySet().forEach(dest -> {
            if (!host.equals(dest)) {
                communicationProblems(host).add(dest);
            }
            communicationProblems(dest).add(host);
        });
    }

    /**
     * Removes any simulated connection issues from and to given {@code host}, allowing normal communication.
     *
     * @param host the host to remove simulated connection issues for
     */
    public void reconnect(String host) {
        communicationProblems(host).clear();
        communicationProblems.values().forEach(v -> v.remove(host));
    }

    /**
     * Removes all simulated network issues between nodes
     */
    public void clearNetworkProblems() {
        communicationProblems.clear();
    }

    /**
     * Creates a network partition containing the given {@code partition} of nodes. This restricts communication
     * between any nodes in this partition with other nodes in the cluster, and vice versa.
     * <p>
     * Note that any previous communication issues are remained intact.
     *
     * @param partition The nodes in the one half of the partitioned network
     */
    public void createNetworkPartition(String... partition) {
        Set<String> onePartition = new HashSet<>(asList(partition));
        Set<String> otherPartition = new HashSet<>(clusterNodes.keySet());
        otherPartition.removeAll(onePartition);
        clusterNodes.keySet().forEach(node -> {
            if (onePartition.contains(node)) {
                communicationProblems(node).addAll(otherPartition);
            } else {
                communicationProblems(node).addAll(onePartition);
            }
        });
    }

    private Set<String> communicationProblems(String host) {
        return communicationProblems.computeIfAbsent(host, k -> new CopyOnWriteArraySet<>());
    }

    public Collection<RaftNode> nodes() {
        return clusterNodes.values();
    }

    public void shutdown() {
        remoteCommunication.shutdownNow();
        clusterNodes.forEach((name,node) -> node.stop());
    }

    private <S, R> CompletableFuture<R> communicateRemote(S request, Function<S, R> replyBuilder, String origin, String destination) {
        CompletableFuture<R> result = new CompletableFuture<>();
        if (!remoteCommunication.isShutdown()) {
            int delay = communicationDelay(destination, origin);
            logger.debug("Sending {} from {} to {} in {}ms", request.getClass().getSimpleName(), origin, destination, delay);
            remoteCommunication.schedule(() -> {
                if (communicationProblems.containsKey(origin) && communicationProblems.get(origin).contains(destination)) {
                    logger.debug("{} from {} to {} was blocked due to simulated communication issues", request.getClass().getSimpleName(), origin, destination);
                    result.completeExceptionally(new IOException("Mocking disconnected node"));
                } else {
                    if (!remoteCommunication.isShutdown()) {
                        logger.debug("{} from {} arrived at {}", request.getClass().getSimpleName(), origin, destination);
                        R reply = replyBuilder.apply(request);
                        remoteCommunication.schedule(() -> result.complete(reply),
                                                     delay, TimeUnit.MILLISECONDS);
                    }
                }
            }, delay, TimeUnit.MILLISECONDS);
        }
        return result;
    }

    private int communicationDelay(String from, String to) {
        return communicationDelay.computeIfAbsent(from, k -> new ConcurrentHashMap<>()).computeIfAbsent(to, k-> defaultDelay).get();
    }

    public void setCommunicationDelay(String from, String to, int minDelay, int maxDelay) {
        communicationDelay.computeIfAbsent(from, k -> new ConcurrentHashMap<>()).put(to, () -> ThreadLocalRandom.current().nextInt(minDelay, maxDelay));
    }

    public void setCommunicationDelay(int minDelay, int maxDelay) {
        defaultDelay = () -> ThreadLocalRandom.current().nextInt(minDelay, maxDelay);
        communicationDelay.forEach((k, v) -> v.replaceAll((t, y) -> defaultDelay));
    }

    private class StubRaftGroup implements RaftGroup, RaftConfiguration {

        private final Map<String, RaftNode> nodes = new ConcurrentHashMap<>();
        private final String localName;
        private final LogEntryStore logEntryStore;
        private final ElectionStore electionStore;
        private final LogEntryProcessor logEntryProcessor;

        public StubRaftGroup(String localName) {
            this.localName = localName;
            this.logEntryStore = new InMemoryLogEntryStore(localName);
            this.electionStore = new InMemoryElectionStore();
            logEntryProcessor = new LogEntryProcessor(new InMemoryProcessorStore());
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
        public RaftPeer peer(Node node) {
            return new StubNode(node.getNodeId());
        }

        @Override
        public RaftNode localNode() {
            return nodes.get(this.localName);
        }

        @Override
        public void delete() {

        }

        @Override
        public List<Node> groupMembers() {
            return nodes.keySet().stream()
                        .map(sn -> Node.newBuilder()
                                       .setHost(sn)
                                       .setNodeId(sn).build())
                        .collect(Collectors.toList());
        }


        @Override
        public String groupId() {
            return "default";
        }

        @Override
        public void update(List<Node> nodes) {
            this.nodes.clear();
            nodes.stream()
                 .map(Node::getHost)
                 .forEach(host -> this.nodes.put(host, new RaftNode(host, this, new FakeSnapshotManager())));
        }

        private void update(Map<String, RaftNode> nodes) {
            this.nodes.clear();
            this.nodes.putAll(nodes);
        }

        private class StubNode implements RaftPeer {

            private final String nodeId;
            private final StubRaftGroup remote;
            private Consumer<AppendEntriesResponse> appendEntriesResponseListener;
            private Consumer<InstallSnapshotResponse> installSnapshotResponseListener;

            public StubNode(String nodeId) {
                this.remote = clusterGroups.get(nodeId);
                this.nodeId = nodeId;
            }

            @Override
            public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
                return communicateRemote(request, remote.localNode()::requestVote, localName, nodeId);
            }

            @Override
            public void appendEntries(AppendEntriesRequest request) {
                communicateRemote(request, r->{
                    AppendEntriesResponse response = remote.localNode().appendEntries(r);
                    appendEntriesResponseListener.accept(response);
                    return null;
                }, localName, nodeId);

            }

            @Override
            public void installSnapshot(InstallSnapshotRequest request) {
                communicateRemote(request, r->{
                    InstallSnapshotResponse response = remote.localNode().installSnapshot(r);
                    installSnapshotResponseListener.accept(response);
                    return null;
                }, localName, nodeId);
            }

            @Override
            public Registration registerAppendEntriesResponseListener(Consumer<AppendEntriesResponse> listener) {
                appendEntriesResponseListener = listener;
                return () -> appendEntriesResponseListener = null;
            }

            @Override
            public Registration registerInstallSnapshotResponseListener(Consumer<InstallSnapshotResponse> listener) {
                installSnapshotResponseListener = listener;
                return () -> installSnapshotResponseListener = null;
            }

            @Override
            public String nodeId() {
                return nodeId;
            }

        }

    }
}
