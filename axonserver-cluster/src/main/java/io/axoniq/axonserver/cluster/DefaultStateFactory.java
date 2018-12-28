package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.snapshot.SnapshotManager;

import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class DefaultStateFactory implements MembershipStateFactory {

    private final RaftGroup raftGroup;
    private final BiConsumer<MembershipState, MembershipState> transitionHandler;
    private final Supplier<Scheduler> schedulerFactory;
    private final SnapshotManager snapshotManager;

    public DefaultStateFactory(RaftGroup raftGroup,
                               BiConsumer<MembershipState, MembershipState> transitionHandler,
                               SnapshotManager snapshotManager) {
        this.raftGroup = raftGroup;
        this.transitionHandler = transitionHandler;
        this.snapshotManager = snapshotManager;
        this.schedulerFactory = DefaultScheduler::new;
    }

    @Override
    public IdleState idleState(String nodeId) {
        return new IdleState(nodeId);
    }

    @Override
    public LeaderState leaderState() {
        return LeaderState.builder()
                          .raftGroup(raftGroup)
                          .schedulerFactory(schedulerFactory)
                          .transitionHandler(transitionHandler)
                          .snapshotManager(snapshotManager)
                          .stateFactory(this)
                          .build();
    }

    @Override
    public FollowerState followerState() {
        return FollowerState.builder()
                            .raftGroup(raftGroup)
                            .schedulerFactory(schedulerFactory)
                            .transitionHandler(transitionHandler)
                            .snapshotManager(snapshotManager)
                            .stateFactory(this)
                            .build();
    }

    @Override
    public CandidateState candidateState() {
        return CandidateState.builder()
                             .raftGroup(raftGroup)
                             .schedulerFactory(schedulerFactory)
                             .transitionHandler(transitionHandler)
                             .snapshotManager(snapshotManager)
                             .stateFactory(this)
                             .build();
    }
}
