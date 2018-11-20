package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.election.ElectionStore;
import io.axoniq.axonserver.cluster.election.InMemoryElectionStore;
import io.axoniq.axonserver.cluster.replication.InMemoryLogEntryStore;
import io.axoniq.axonserver.cluster.replication.LogEntryStore;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.AppendEntryFailure;
import io.axoniq.axonserver.grpc.cluster.AppendEntrySuccess;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RaftClusterTestFixture {

    private final ScheduledExecutorService remoteCommunication = new ScheduledThreadPoolExecutor(2);
    private Map<String, StubRaftGroup> clusterGroups = new ConcurrentHashMap<>();
    private Map<String, RaftNode> clusterNodes = new ConcurrentHashMap<>();
    private Map<String, Set<String>> communicationProblems = new ConcurrentHashMap<>();

    public RaftClusterTestFixture(String... hostNames) {
        for (int i = 0; i < hostNames.length; i++) {
            String hostName = hostNames[i];
            StubRaftGroup raftGroup = new StubRaftGroup(hostName);
            clusterGroups.put(hostName, new StubRaftGroup(hostName));
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
    public void createNetworkPartition(Set<String> partition) {
        Set<String> otherPartition = new HashSet<>(clusterNodes.keySet());
        otherPartition.removeAll(partition);
        clusterNodes.keySet().forEach(node -> {
            if (partition.contains(node)) {
                communicationProblems(node).addAll(otherPartition);
            } else {
                communicationProblems(node).addAll(partition);
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
                result.complete(replyBuilder.apply(request));
            }
        }, (long) ThreadLocalRandom.current().nextInt(10, 20), TimeUnit.MILLISECONDS);
        return result;
    }

    private long communicationDelay(String from, String to) {
        return ThreadLocalRandom.current().nextInt(10, 20);
    }

    private class StubRaftGroup implements RaftGroup, RaftConfiguration {

        private final String localName;
        private final LogEntryStore logEntryStore;
        private final ElectionStore electionStore;
        private AtomicReference<Function<AppendEntriesRequest, AppendEntriesResponse>> appendEntriesHandler = new AtomicReference<>();
        private AtomicReference<Function<InstallSnapshotRequest, InstallSnapshotResponse>> installSnapshotHandler = new AtomicReference<>();
        private AtomicReference<Function<RequestVoteRequest, RequestVoteResponse>> requestVoteHandler = new AtomicReference<>();

        public StubRaftGroup(String localName) {
            this.localName = localName;
            this.logEntryStore = new InMemoryLogEntryStore();
            this.electionStore = new InMemoryElectionStore();
        }

        @Override
        public Registration onAppendEntries(Function<AppendEntriesRequest, AppendEntriesResponse> handler) {
            appendEntriesHandler.set(handler);
            return () -> appendEntriesHandler.compareAndSet(handler, null);
        }

        @Override
        public Registration onInstallSnapshot(Function<InstallSnapshotRequest, InstallSnapshotResponse> handler) {
            installSnapshotHandler.set(handler);
            return () -> installSnapshotHandler.compareAndSet(handler, null);

        }

        @Override
        public Registration onRequestVote(Function<RequestVoteRequest, RequestVoteResponse> handler) {
            requestVoteHandler.set(handler);
            return () -> requestVoteHandler.compareAndSet(handler, null);
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
            return new FakeRaftPeer(nodeId);
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

    }
}
