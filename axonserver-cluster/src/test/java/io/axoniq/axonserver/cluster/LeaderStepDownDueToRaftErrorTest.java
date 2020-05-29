package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.election.InMemoryElectionStore;
import io.axoniq.axonserver.cluster.exception.ErrorCode;
import io.axoniq.axonserver.cluster.replication.InMemoryLogEntryStore;
import io.axoniq.axonserver.cluster.snapshot.FakeSnapshotManager;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.AppendEntryFailure;
import io.axoniq.axonserver.grpc.cluster.Node;
import org.jetbrains.annotations.NotNull;
import org.junit.*;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Tests various cases when leader should step down.
 *
 * @author Milan Savic
 */
public class LeaderStepDownDueToRaftErrorTest {

    @Test
    public void stepDownDueToInvalidLeaderResponseReceived() {
        RaftPeer follower = mock(RaftPeer.class);
        when(follower.nodeId()).thenReturn("node-1");
        when(follower.nodeName()).thenReturn("node-1");
        AtomicReference<Consumer<AppendEntriesResponse>> appendEntriesResponseConsumerRef = new AtomicReference<>();
        when(follower.registerAppendEntriesResponseListener(any())).thenAnswer(a -> {
            appendEntriesResponseConsumerRef.set(a.getArgument(0));
            return null;
        });
        FakeTransitionHandler transitionHandler = new FakeTransitionHandler();
        LeaderState leaderState = LeaderState.builder()
                                             .transitionHandler(transitionHandler)
                                             .snapshotManager(new FakeSnapshotManager())
                                             .raftGroup(raftGroup(follower))
                                             .termUpdateHandler(mock(BiConsumer.class))
                                             .stateFactory(new FakeStateFactory())
                                             .matchStrategy(i -> false)
                                             .build();
        leaderState.start();
        AppendEntryFailure appendEntryFailure = AppendEntryFailure.newBuilder()
                                                                  .setErrorCode(ErrorCode.INVALID_LEADER.code())
                                                                  .setCause("oops")
                                                                  .build();
        appendEntriesResponseConsumerRef.get().accept(AppendEntriesResponse.newBuilder()
                                                                           .setFailure(appendEntryFailure)
                                                                           .build());

        assertEquals("follower", ((FakeStateFactory.FakeState) transitionHandler.lastTransition()).name());
    }

    @NotNull
    private RaftGroup raftGroup(RaftPeer follower) {
        RaftGroup raftGroup = mock(RaftGroup.class);
        when(raftGroup.localLogEntryStore()).thenReturn(new InMemoryLogEntryStore("test"));
        when(raftGroup.localElectionStore()).thenReturn(new InMemoryElectionStore());
        when(raftGroup.logEntryProcessor()).thenReturn(mock(LogEntryProcessor.class));
        when(raftGroup.createIterator()).thenReturn(Collections.emptyIterator());
        when(raftGroup.peer(any())).thenReturn(follower);
        RaftNode localNode = mock(RaftNode.class);
        when(localNode.nodeId()).thenReturn("node-0");
        when(raftGroup.localNode()).thenReturn(localNode);
        FakeRaftConfiguration raftConfiguration = new FakeRaftConfiguration("test", "node-0");
        raftConfiguration.addNode(Node.newBuilder()
                                      .setNodeId(follower.nodeId())
                                      .build());
        when(raftGroup.raftConfiguration()).thenReturn(raftConfiguration);
        return raftGroup;
    }
}
