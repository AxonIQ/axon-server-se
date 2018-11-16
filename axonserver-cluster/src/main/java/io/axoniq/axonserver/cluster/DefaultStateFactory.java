package io.axoniq.axonserver.cluster;

import java.util.function.Consumer;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class DefaultStateFactory implements MembershipStateFactory {

    private final RaftGroup raftGroup;

    private final Consumer<MembershipState> transitionHandler;

    public DefaultStateFactory(RaftGroup raftGroup,
                               Consumer<MembershipState> transitionHandler) {
        this.raftGroup = raftGroup;
        this.transitionHandler = transitionHandler;
    }

    @Override
    public IdleState idleState() {
        return new IdleState();
    }

    @Override
    public LeaderState leaderState() {
        return LeaderState.builder()
                          .raftGroup(raftGroup)
                          .transitionHandler(transitionHandler)
                          .stateFactory(this)
                          .build();
    }

    @Override
    public FollowerState followerState() {
        return FollowerState.builder()
                            .raftGroup(raftGroup)
                            .transitionHandler(transitionHandler)
                            .stateFactory(this)
                            .build();
    }

    @Override
    public CandidateState candidateState() {
        return CandidateState.builder()
                             .raftGroup(raftGroup)
                             .transitionHandler(transitionHandler)
                             .stateFactory(this)
                             .build();
    }
}
