package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.election.ElectionStore;
import io.axoniq.axonserver.cluster.election.InMemoryElectionStore;
import io.axoniq.axonserver.cluster.exception.LeadershipTransferInProgressException;
import io.axoniq.axonserver.cluster.replication.InMemoryLogEntryStore;
import io.axoniq.axonserver.cluster.replication.LogEntryStore;
import io.axoniq.axonserver.cluster.scheduler.DefaultScheduler;
import io.axoniq.axonserver.cluster.snapshot.FakeSnapshotManager;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.AppendEntrySuccess;
import io.axoniq.axonserver.grpc.cluster.ConfigChangeResult;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
import io.axoniq.axonserver.grpc.cluster.Role;
import org.junit.*;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class LeaderStateTest {

    private LeaderState testSubject;

    private final AtomicReference<String> timeoutTarget = new AtomicReference<>();
    private final AtomicInteger responseDelay = new AtomicInteger(1000);
    private final FakeScheduler fakeScheduler = new FakeScheduler();
    private final AtomicReference<MembershipState> stateRef = new AtomicReference<>();
    private RaftGroup raftGroup;

    @Before
    public void setup() {
        StateTransitionHandler transitionHandler = (old, state, cause) -> stateRef.set(state);

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
                groupMembers = nodes;

            }
        };
        LogEntryStore logEntryStore = new InMemoryLogEntryStore("Test");
        AtomicReference<RaftNode> nodeRef = new AtomicReference<>();

        ElectionStore electionStore = new InMemoryElectionStore();
        LogEntryProcessor logEntryProcessor = new LogEntryProcessor(new InMemoryProcessorStore());
        BiConsumer<Long,String> termUpdateHandler = (newTerm, cause) -> electionStore.updateCurrentTerm(newTerm);
        raftGroup = new RaftGroup() {
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
            public RaftPeer peer(Node node) {
                return new RaftPeer() {
                    Consumer<AppendEntriesResponse> listener;

                    @Override
                    public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
                        return null;
                    }

                    @Override
                    public CompletableFuture<RequestVoteResponse> requestPreVote(RequestVoteRequest request) {
                        return null;
                    }

                    @Override
                    public void appendEntries(AppendEntriesRequest request) {
                        AppendEntriesResponse response = AppendEntriesResponse.newBuilder()
                                                                              .setSuccess(AppendEntrySuccess.newBuilder()
                                                                                                            .setLastLogIndex(request.getPrevLogIndex() + request.getEntriesCount())
                                                                                                            .build())
                                                                              .build();
                        if (responseDelay.get() == 0) {
                            listener.accept(response);
                        } else {
                            fakeScheduler.schedule(() -> listener.accept(response),
                                                   responseDelay.get(),
                                                   TimeUnit.MILLISECONDS);
                        }
                    }

                    @Override
                    public void installSnapshot(InstallSnapshotRequest request) {

                    }

                    @Override
                    public Registration registerAppendEntriesResponseListener(
                            Consumer<AppendEntriesResponse> listener) {
                        this.listener = listener;
                        return () -> this.listener = null;
                    }

                    @Override
                    public Registration registerInstallSnapshotResponseListener(
                            Consumer<InstallSnapshotResponse> listener) {
                        return null;
                    }

                    @Override
                    public String nodeId() {
                        return node.getNodeId();
                    }

                    @Override
                    public String nodeName() {
                        return node.getNodeName();
                    }

                    @Override
                    public void sendTimeoutNow() {
                        timeoutTarget.set(node.getNodeId());
                    }

                    @Override
                    public boolean primaryNode() {
                        return true;
                    }

                    @Override
                    public boolean votingNode() {
                        return true;
                    }

                    @Override
                    public Role role() {
                        return Role.PRIMARY;
                    }
                };
            }

            @Override
            public RaftNode localNode() {
                return nodeRef.get();
            }
        };

        nodeRef.set(new RaftNode("test", raftGroup, new FakeSnapshotManager()));

        testSubject = LeaderState.builder()
                                 .transitionHandler(transitionHandler)
                                 .termUpdateHandler(termUpdateHandler)
                                 .raftGroup(raftGroup)
                                 .snapshotManager(new FakeSnapshotManager())
                                 .scheduler(fakeScheduler)
                                 .matchStrategy(nextCommitCandidate -> true)
                                 .stateFactory(new DefaultStateFactory(raftGroup, transitionHandler,
                                                                       termUpdateHandler,
                                                                       new FakeSnapshotManager()))
                                 .build();
    }

    @Test
    public void startAndStop() throws InterruptedException {
        testSubject.start();
        Thread.sleep(10);
        fakeScheduler.timeElapses(500);
        assertTrue(stateRef.get() instanceof FollowerState);
    }

    @Test
    public void transferLeadership() throws Exception {
        responseDelay.set(5);
        testSubject.start();
        Thread.sleep(10);

        testSubject.appendEntry("Sample", "Sample".getBytes());

        CompletableFuture<Void> transferDone = testSubject.transferLeadership();
        try {
            testSubject.appendEntry("Sample", "Sample".getBytes());
            fail("Cannot append entry when transferring leadership");
        } catch( LeadershipTransferInProgressException ex) {
        }

        fakeScheduler.timeElapses(100);

        assertNotNull(timeoutTarget.get());
        assertTrue(transferDone.isDone());
    }

    @Test
    public void addNode() throws Exception {
        responseDelay.set(0);

        LogEntryApplier logEntryApplier = new LogEntryApplier(raftGroup,
                                                              new DefaultScheduler(),
                                                              testSubject::applied,
                                                              c -> raftGroup.raftConfiguration()
                                                                            .update(c.getNodesList()));
        try {
            logEntryApplier.start();

            testSubject.start();

            CompletableFuture<ConfigChangeResult> configChangeResultCompletableFuture = testSubject
                    .addServer(Node.newBuilder()
                                   .setNodeId("NewNode")
                                   .setHost("localhost")
                                   .setPort(1234)
                                   .setNodeName("NewNodeName")
                                   .setRole(Role.ACTIVE_BACKUP)
                                   .build());

            ConfigChangeResult configuration = configChangeResultCompletableFuture.get(15, TimeUnit.SECONDS);

            assertTrue(configuration.hasSuccess());

            Optional<Node> optionalNode = raftGroup.raftConfiguration().groupMembers().stream().filter(n -> n
                    .getNodeId().equals("NewNode")).findFirst();

            assertTrue(optionalNode.isPresent());
            assertEquals(Role.ACTIVE_BACKUP, optionalNode.get().getRole());
        } finally {
            logEntryApplier.stop();
        }
    }
}