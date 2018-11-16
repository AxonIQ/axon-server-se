package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.election.ElectionStore;
import io.axoniq.axonserver.cluster.replication.InMemoryLogEntryStore;
import io.axoniq.axonserver.cluster.replication.LogEntryStore;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
import org.junit.*;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Author: marc
 */
public class LeaderStateTest {

    private LeaderState testSubject;

    @Before
    public void setup() {

        RaftGroup raftGroup = new RaftGroup() {

            private LogEntryStore localLogEntryStore = new InMemoryLogEntryStore();

            private RaftConfiguration raftConfiguration = new RaftConfiguration() {
                List<Node> nodes = Arrays.asList(Node.newBuilder().setNodeId("Test").build(), Node.newBuilder().setNodeId("Test2").build());
                @Override
                public List<Node> groupMembers() {
                    return nodes;
                }
            };

            ElectionStore localElectionStore = new ElectionStore() {
                @Override
                public String votedFor() {
                    return null;
                }

                @Override
                public void markVotedFor(String candidate) {

                }

                @Override
                public long currentTerm() {
                    return 10;
                }

                @Override
                public void updateCurrentTerm(long term) {

                }
            };

            
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
                return localLogEntryStore;
            }

            @Override
            public ElectionStore localElectionStore() {
                return localElectionStore;
            }

            @Override
            public RaftConfiguration raftConfiguration() {
                return raftConfiguration;
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
            public ReplicationConnection createReplicationConnection(String nodeId, Consumer<Long> matchIndexListener) {
                return new DummyReplicationConnection(localLogEntryStore, matchIndexListener);
            }
        };
        testSubject = new LeaderState(new AbstractMembershipState.Builder().raftGroup(raftGroup).transitionHandler(newState -> {}));
    }

    @Test
    public void startAndStop() throws InterruptedException {
        testSubject.appendEntry("Test", "Hello".getBytes());
        testSubject.appendEntry("Test", "Hello".getBytes());
        testSubject.appendEntry("Test", "Hello".getBytes());
        testSubject.start();
        Thread.sleep(TimeUnit.MILLISECONDS.toMillis(500));
        testSubject.appendEntry("Test", "Hello".getBytes());
        Thread.sleep(TimeUnit.MILLISECONDS.toMillis(500));
        testSubject.appendEntry("Test", "Hello".getBytes());
        testSubject.appendEntry("Test", "Hello".getBytes());
        testSubject.appendEntry("Test", "Hello".getBytes());
        Thread.sleep(TimeUnit.SECONDS.toMillis(5));
        testSubject.stop();
    }
}