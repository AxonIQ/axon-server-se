package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.election.ElectionStore;
import io.axoniq.axonserver.cluster.election.InMemoryElectionStore;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.Assert.assertTrue;

/**
 * Author: marc
 */
public class LeaderStateTest {

    private LeaderState testSubject;
    AtomicReference<RaftNode> nodeRef = new AtomicReference<>();
    FakeScheduler fakeScheduler = new FakeScheduler();
    AtomicReference<MembershipState> stateRef = new AtomicReference<>();

    @Before
    public void setup() {
        Consumer<MembershipState> transitionHandler = (state) -> stateRef.set(state);

        RaftConfiguration raftConfiguration = new RaftConfiguration() {
            private List<Node> groupMembers = Arrays.asList(Node.newBuilder().setNodeId("test").build(),
                                                            Node.newBuilder().setNodeId("Other").build());
            @Override
            public List<Node> groupMembers() {
                return groupMembers;
            }

            @Override
            public String groupId() {
                return "MyGroup";
            }

            @Override
            public void update(List<Node> nodes) {

            }
        };
        LogEntryStore logEntryStore = new InMemoryLogEntryStore("Test");
        AtomicReference<RaftNode> nodeRef = new AtomicReference<>();

        ElectionStore electionStore = new InMemoryElectionStore();
        LogEntryProcessor logEntryProcessor = new LogEntryProcessor(new InMemoryProcessorStore());

        RaftGroup raftGroup = new RaftGroup() {
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
                return raftConfiguration;
            }

            @Override
            public LogEntryProcessor logEntryProcessor() {
                return logEntryProcessor;
            }

            @Override
            public RaftPeer peer(String nodeId) {
                return new RaftPeer() {
                    @Override
                    public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
                        return null;
                    }

                    @Override
                    public void appendEntries(AppendEntriesRequest request) {

                    }

                    @Override
                    public void installSnapshot(InstallSnapshotRequest request) {

                    }

                    @Override
                    public Registration registerAppendEntriesResponseListener(
                            Consumer<AppendEntriesResponse> listener) {
                        return null;
                    }

                    @Override
                    public Registration registerInstallSnapshotResponseListener(
                            Consumer<InstallSnapshotResponse> listener) {
                        return null;
                    }

                    @Override
                    public String nodeId() {
                        return nodeId;
                    }
                };
            }

            @Override
            public RaftNode localNode() {
                return nodeRef.get();
            }
        };

        nodeRef.set(new RaftNode("test", raftGroup));

        testSubject = LeaderState.builder()
                .transitionHandler(transitionHandler)
                .raftGroup(raftGroup)
                .scheduler(fakeScheduler)
                .stateFactory(new DefaultStateFactory(raftGroup, transitionHandler))
                .build();
    }

    @Test
    public void startAndStop() throws InterruptedException, TimeoutException, ExecutionException {
        testSubject.start();
        Thread.sleep(10);
        fakeScheduler.timeElapses(500);
        assertTrue(stateRef.get() instanceof FollowerState);
    }
}