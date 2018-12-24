package io.axoniq.axonserver.cluster;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class DefaultStateFactory implements MembershipStateFactory {

    private final RaftGroup raftGroup;

    private final BiConsumer<MembershipState, MembershipState> transitionHandler;
    private final Supplier<Scheduler> schedulerFactory;

    public DefaultStateFactory(RaftGroup raftGroup,
                               BiConsumer<MembershipState, MembershipState> transitionHandler) {
        this.raftGroup = raftGroup;
        this.transitionHandler = transitionHandler;
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
                          .stateFactory(this)
                          .build();
    }

    @Override
    public FollowerState followerState() {
        return FollowerState.builder()
                            .raftGroup(raftGroup)
                            .schedulerFactory(schedulerFactory)
                            .transitionHandler(transitionHandler)
                            .stateFactory(this)
                            .build();
    }

    @Override
    public CandidateState candidateState() {
        return CandidateState.builder()
                             .raftGroup(raftGroup)
                             .schedulerFactory(schedulerFactory)
                             .transitionHandler(transitionHandler)
                             .stateFactory(this)
                             .build();
    }
}
