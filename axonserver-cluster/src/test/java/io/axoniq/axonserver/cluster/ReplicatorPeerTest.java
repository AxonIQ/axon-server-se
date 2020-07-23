package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.election.InMemoryElectionStore;
import io.axoniq.axonserver.cluster.replication.InMemoryLogEntryStore;
import io.axoniq.axonserver.cluster.snapshot.SnapshotContext;
import io.axoniq.axonserver.cluster.snapshot.SnapshotManager;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.AppendEntryFailure;
import io.axoniq.axonserver.grpc.cluster.AppendEntrySuccess;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotSuccess;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.RequestIncrementalData;
import io.axoniq.axonserver.grpc.cluster.ResponseHeader;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import org.junit.*;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class ReplicatorPeerTest {

    private ReplicatorPeer testSubject;
    AtomicLong lastIndexInPeer = new AtomicLong();
    AtomicBoolean supportsReplicationGroup = new AtomicBoolean(true);
    RaftNode raftNode = mock(RaftNode.class);
    RaftPeer raftPeer = mock(RaftPeer.class);

    @Before
    public void setUp() throws Exception {
        SnapshotManager snapshotManager = new SnapshotManager() {
            @Override
            public Flux<SerializedObject> streamSnapshotData(SnapshotContext installationContext) {
                return Flux.fromIterable(Arrays.asList(SerializedObject.newBuilder().setType("Type1").build()));
            }

            @Override
            public Flux<SerializedObject> streamAppendOnlyData(SnapshotContext installationContext) {
                return Flux.fromIterable(Arrays.asList(SerializedObject.newBuilder().setType("Type2").build()));
            }

            @Override
            public Mono<Void> applySnapshotData(SerializedObject serializedObject, Role peerRole) {
                return Mono.empty();
            }

            @Override
            public void clear() {

            }
        };
        EmitterProcessor<Long> processor = EmitterProcessor.create(10);
        FluxSink<Long> matchIndexUpdates = processor.sink();
        processor.subscribe(matchIndex -> {
        });
        Clock clock = new FakeClock(Instant.now());
        BiConsumer<Long, String> updateCurrentTerm = (term, reason) -> {
        };
        InMemoryLogEntryStore logEntryStore = new InMemoryLogEntryStore("TEST");
        Supplier<Long> lastLogIndexProvider = () -> logEntryStore.lastLogIndex();


        when(raftPeer.nodeId()).thenReturn("PeerNodeId");
        when(raftPeer.nodeName()).thenReturn("PeerNodeName");
        when(raftPeer.isReadyForAppendEntries()).thenReturn(true);
        when(raftPeer.isReadyForSnapshot()).thenReturn(true);
        when(raftPeer.role()).thenReturn(Role.PRIMARY);
        RaftGroup raftGroup = mock(RaftGroup.class);
        RaftConfiguration raftConfiguration = new RaftConfiguration() {
            @Override
            public String groupId() {
                return "TEST";
            }

            @Override
            public List<Node> groupMembers() {
                return Collections.emptyList();
            }

            @Override
            public void update(List<Node> newConf) {
            }

            @Override
            public boolean forceSnapshotOnJoin() {
                return true;
            }
        };
        logEntryStore.createEntry(1, "TestEntry", "testData".getBytes());
        logEntryStore.createEntry(1, "TestEntry2", "testData2".getBytes());
        when(raftGroup.localLogEntryStore()).thenReturn(logEntryStore);
        when(raftGroup.localElectionStore()).thenReturn(new InMemoryElectionStore());
        when(raftGroup.logEntryProcessor()).thenReturn(new LogEntryProcessor(null) {
            @Override
            public long lastAppliedIndex() {
                return logEntryStore.lastLogIndex();
            }

            @Override
            public long lastAppliedTerm() {
                return 1;
            }

            @Override
            public long commitIndex() {
                return 0;
            }

            @Override
            public long commitTerm() {
                return 1;
            }
        });
        when(raftGroup.raftConfiguration()).thenReturn(raftConfiguration);
        when(raftNode.nodeId()).thenReturn("LocalNodeId");
        when(raftGroup.localNode()).thenReturn(raftNode);

        AtomicReference<Consumer<AppendEntriesResponse>> responseListener = new AtomicReference<>();
        AtomicReference<Consumer<InstallSnapshotResponse>> installSnapshotResponseListener = new AtomicReference<>();
        when(raftPeer.registerAppendEntriesResponseListener(any())).thenAnswer(invocation -> {
            responseListener.set(invocation.getArgument(0));
            return new Registration() {
                @Override
                public void cancel() {
                    responseListener.set(null);
                }
            };
        });
        when(raftPeer.registerInstallSnapshotResponseListener(any())).thenAnswer(invocation -> {
            installSnapshotResponseListener.set(invocation.getArgument(0));
            return new Registration() {
                @Override
                public void cancel() {
                    installSnapshotResponseListener.set(null);
                }
            };
        });
        doAnswer(invocation -> {
            AppendEntriesRequest request = (invocation.getArgument(0));
            System.out.println(request.getPrevLogIndex() + " == " + lastIndexInPeer.get());
            AppendEntriesResponse.Builder response = AppendEntriesResponse.newBuilder()
                                                                          .setTerm(request.getTerm())
                                                                          .setGroupId(request.getGroupId())
                                                                          .setResponseHeader(ResponseHeader.newBuilder()
                                                                                                           .setNodeId(
                                                                                                                   "Node1")
                                                                                                           .setRequestId(
                                                                                                                   request.getRequestId())
                                                                                                           .setResponseId(
                                                                                                                   UUID.randomUUID()
                                                                                                                       .toString())
                                                                                                           .build());

            if (request.getPrevLogIndex() == lastIndexInPeer.get()) {
                response.setSuccess(AppendEntrySuccess.newBuilder()
                                                      .setLastLogIndex(lastIndexInPeer
                                                                               .addAndGet(request.getEntriesCount()))
                                                      .build());
            } else {
                response.setFailure(AppendEntryFailure.newBuilder()
                                                      .setLastAppliedIndex(lastIndexInPeer.get())
                                                      .setCause("Term/Index not found " + request.getPrevLogTerm() + "/"
                                                                        + request.getPrevLogIndex())
                                                      .setSupportsReplicationGroups(supportsReplicationGroup.get())
                                                      .build());
            }
            responseListener.get().accept(response.build());
            return null;
        }).when(raftPeer).appendEntries(any(AppendEntriesRequest.class));

        doAnswer(invocation -> {
            InstallSnapshotRequest request = (invocation.getArgument(0));
            InstallSnapshotResponse.Builder response = InstallSnapshotResponse.newBuilder()
                                                                              .setTerm(request.getTerm())
                                                                              .setGroupId(request.getGroupId())
                                                                              .setResponseHeader(ResponseHeader
                                                                                                         .newBuilder()
                                                                                                         .setNodeId(
                                                                                                                 "Node1")
                                                                                                         .setRequestId(
                                                                                                                 request.getRequestId())
                                                                                                         .setResponseId(
                                                                                                                 UUID.randomUUID()
                                                                                                                     .toString())
                                                                                                         .build());

            if (request.getConfigDone()) {
                response.setRequestIncrementalData(RequestIncrementalData.newBuilder()
                                                                         .setLastReceivedOffset(request.getOffset())
                                                                         .build());
            } else {
                response.setSuccess(InstallSnapshotSuccess.newBuilder()
                                                          .setLastReceivedOffset(request.getOffset())
                                                          .build());
            }
            if (request.getDone()) {
                lastIndexInPeer.set(request.getLastIncludedIndex());
            }
            installSnapshotResponseListener.get().accept(response.build());
            return null;
        }).when(raftPeer).installSnapshot(any(InstallSnapshotRequest.class));


        testSubject = new ReplicatorPeer(raftPeer,
                                         matchIndexUpdates,
                                         clock,
                                         raftGroup,
                                         snapshotManager,
                                         updateCurrentTerm,
                                         lastLogIndexProvider,
                                         stepDown -> {
                                         });
    }

    @Test
    public void startInAppendEntryState() {
        lastIndexInPeer.set(1);
        testSubject.start();
        while (testSubject.sendNextMessage() > 0) {
            System.out.println("Send more");
        }
        assertEquals(3, testSubject.nextIndex());
    }

    @Test
    public void startInAppendEntryStateLastIndexMissing() {
        testSubject.start();
        while (testSubject.sendNextMessage() > 0) {
            System.out.println("Send more");
        }
        assertEquals(3, testSubject.nextIndex());
    }

    @Test
    public void startInAppendEntryStateLastIndexMissingOldVersion() {
        supportsReplicationGroup.set(false);
        testSubject.start();
        while (testSubject.sendNextMessage() > 0) {
            System.out.println("Send more");
        }
        assertEquals(3, testSubject.nextIndex());
    }
}