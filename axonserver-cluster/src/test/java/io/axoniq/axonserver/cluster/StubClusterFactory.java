package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.election.ElectionStore;
import io.axoniq.axonserver.cluster.replication.LogEntryStore;
import io.axoniq.axonserver.grpc.cluster.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

public class StubClusterFactory {

    private final String localName;
    private StubRaftGroup raftGroup;
    private Map<String, PeerBehavior> nodes = new HashMap<>();

    public StubClusterFactory(String localName) {
        this.localName = localName;
    }

    RaftGroup buildRaftGroup() {
        if (raftGroup == null) {
            this.raftGroup = new StubRaftGroup(localName, nodes);
        }
        return raftGroup;
    }

    void disconnect(String host) {
        raftGroup.disconnect(host);
    }

    void reconnect(String host) {
        raftGroup.reconnect(host);
    }

    void addNode(String host, PeerBehavior behavior) {
        if (raftGroup != null) {
            throw new IllegalStateException("RaftGroup is already created.");
        }
        this.nodes.put(host, behavior);
    }

    private class StubRaftGroup implements RaftGroup, RaftConfiguration {

        private final String localName;
        private Map<String, StubNode> nodes = new HashMap<>();

        public StubRaftGroup(String localName, Map<String, PeerBehavior> mockPeers) {
            this.localName = localName;
            mockPeers.forEach((k, v) -> nodes.put(k, new StubNode(k, v)));
            nodes.put(localName, new StubNode(localName, null));
        }

        @Override
        public Registration onAppendEntries(Function<AppendEntriesRequest, AppendEntriesResponse> handler) {
            return null;
        }

        @Override
        public Registration onInstallSnapshot(Function<InstallSnapshotRequest, InstallSnapshotResponse> handler) {
            return null;
        }

        @Override
        public Registration onRequestVote(Function<RequestVoteRequest, RequestVoteResponse> handler) {
            return null;
        }

        @Override
        public LogEntryStore localLogEntryStore() {
            return null;
        }

        @Override
        public ElectionStore localElectionStore() {
            return null;
        }

        @Override
        public CompletableFuture<ElectionResult> startElection() {
            return null;
        }

        @Override
        public RaftConfiguration raftConfiguration() {
            return this;
        }

        @Override
        public RaftPeer peer(String hostName, int port) {
            return null;
        }

        @Override
        public RaftNode localNode() {
            return null;
        }

        @Override
        public List<Node> groupMembers() {
            return this.nodes.values().stream()
                             .map(sn -> Node.newBuilder()
                                            .setHost(sn.nodeId)
                                            .setNodeId(sn.nodeId).build())
                             .collect(Collectors.toList());
        }

        public void disconnect(String host) {
            nodes.get(host).disconnect();
        }

        public void reconnect(String host) {
            nodes.get(host).reconnect();
        }

        private class StubNode implements RaftPeer {

            private final String nodeId;
            private final PeerBehavior mock;
            private volatile boolean connected = true;

            public StubNode(String nodeId, PeerBehavior mock) {
                this.nodeId = nodeId;
                this.mock = mock;
            }

            public void disconnect() {
                this.connected = false;
            }

            public void reconnect() {
                this.connected = true;
            }

            @Override
            public CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest request) {
                CompletableFuture<AppendEntriesResponse> result = new CompletableFuture<>();
                if (connected) {
                    result.complete(mock.appendEntries(request));
                } else {
                    result.completeExceptionally(new IOException("Mocking disconnected node"));
                }
                return result;
            }

            @Override
            public CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest request) {
                CompletableFuture<InstallSnapshotResponse> result = new CompletableFuture<>();
                if (connected) {
                    result.complete(mock.installSnapshot(request));
                } else {
                    result.completeExceptionally(new IOException("Mocking disconnected node"));
                }
                return result;
            }

            @Override
            public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
                CompletableFuture<RequestVoteResponse> result = new CompletableFuture<>();
                if (connected) {
                    result.complete(mock.requestVote(request));
                } else {
                    result.completeExceptionally(new IOException("Mocking disconnected node"));
                }
                return result;
            }

            @Override
            public String nodeId() {
                return nodeId;
            }
        }

    }
}
