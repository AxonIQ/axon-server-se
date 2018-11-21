package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.election.ElectionStore;
import io.axoniq.axonserver.cluster.election.InMemoryElectionStore;
import io.axoniq.axonserver.cluster.replication.InMemoryLogEntryStore;
import io.axoniq.axonserver.cluster.replication.LogEntryStore;
import io.axoniq.axonserver.grpc.cluster.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

public class RaftClusterTestFixture {

    private final ScheduledExecutorService remoteCommunication = new ScheduledThreadPoolExecutor(2);

    private Map<String, StubRaftGroup> clusterGroups = new ConcurrentHashMap<>();
    private Map<String, RaftNode> clusterNodes = new ConcurrentHashMap<>();
    private Map<String, Set<String>> communicationProblems = new ConcurrentHashMap<>();
    private Map<String, Map<String, Supplier<Integer>>> communicationDelay = new ConcurrentHashMap<>();
    private Supplier<Integer> defaultDelay = () -> ThreadLocalRandom.current().nextInt(10, 20);

    public RaftClusterTestFixture(String... hostNames) {
        for (int i = 0; i < hostNames.length; i++) {
            String hostName = hostNames[i];
            StubRaftGroup raftGroup = new StubRaftGroup(hostName);
            clusterGroups.put(hostName, raftGroup);
            clusterNodes.put(hostName, new RaftNode(hostName, raftGroup));
        }
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

    /**
     * Simulate communication problems for the given {@code host} by blocking all communication from and to
     * it.
     *
     * @param host The host to simulate connection issues for
     */
    public void disconnect(String host) {
        clusterNodes.keySet().forEach(dest -> {
            if (!host.equals(dest))
                communicationProblems(host).add(dest);
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
    }

    private <S, R> CompletableFuture<R> communicateRemote(S request, Function<S, R> replyBuilder, String origin, String destination) {
        CompletableFuture<R> result = new CompletableFuture<>();
        remoteCommunication.schedule(() -> {
            if (communicationProblems.containsKey(origin) && communicationProblems.get(origin).contains(destination)) {
                result.completeExceptionally(new IOException("Mocking disconnected node"));
            } else {
                remoteCommunication.schedule(() -> result.complete(replyBuilder.apply(request)),
                                             communicationDelay(destination, origin), TimeUnit.MILLISECONDS);
            }
        }, communicationDelay(origin, destination), TimeUnit.MILLISECONDS);
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

        private final String localName;
        private final LogEntryStore logEntryStore;
        private final ElectionStore electionStore;

        public StubRaftGroup(String localName) {
            this.localName = localName;
            this.logEntryStore = new InMemoryLogEntryStore();
            this.electionStore = new InMemoryElectionStore();
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
            return new StubNode(nodeId);
        }

        @Override
        public RaftNode localNode() {
            return clusterNodes.get(this.localName);
        }

        @Override
        public List<Node> groupMembers() {
            return RaftClusterTestFixture.this.clusterNodes.keySet().stream()
                                                           .map(sn -> Node.newBuilder()
                                                                          .setHost(sn)
                                                                          .setNodeId(sn).build())
                                                           .collect(Collectors.toList());
        }


        @Override
        public String groupId() {
            return "default";
        }

        private class StubNode implements RaftPeer {

            private final String nodeId;
            private final StubRaftGroup remote;

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

            }

            @Override
            public void installSnapshot(InstallSnapshotRequest request) {

            }

            @Override
            public Registration registerAppendEntriesResponseListener(Consumer<AppendEntriesResponse> listener) {
                return null;
            }

            @Override
            public Registration registerInstallSnapshotResponseListener(Consumer<InstallSnapshotResponse> listener) {
                return null;
            }

            @Override
            public String nodeId() {
                return nodeId;
            }

        }

    }
}
