package io.axoniq.axonserver.cluster;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class DefaultStateFactory implements MembershipStateFactory {

    private final RaftGroup raftGroup;

    private final BiConsumer<MembershipState, MembershipState> transitionHandler;
    private final Scheduler scheduler;

    public DefaultStateFactory(RaftGroup raftGroup,
                               BiConsumer<MembershipState, MembershipState> transitionHandler) {
        this.raftGroup = raftGroup;
        this.transitionHandler = transitionHandler;
        this.scheduler = new DefaultScheduler();
    }

    @Override
    public IdleState idleState(String nodeId) {
        return new IdleState(nodeId);
    }

    @Override
    public LeaderState leaderState() {
        return LeaderState.builder()
                          .raftGroup(raftGroup)
                          .scheduler(scheduler)
                          .transitionHandler(transitionHandler)
                          .stateFactory(this)
                          .build();
    }

    @Override
    public FollowerState followerState() {
        return FollowerState.builder()
                            .raftGroup(raftGroup)
                            .scheduler(scheduler)
                            .transitionHandler(transitionHandler)
                            .stateFactory(this)
                            .build();
    }

    @Override
    public CandidateState candidateState() {
        return CandidateState.builder()
                             .raftGroup(raftGroup)
                             .scheduler(scheduler)
                             .transitionHandler(transitionHandler)
                             .stateFactory(this)
                             .build();
    }
}
