package io.axoniq.axonserver.cluster;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public interface MembershipStateFactory {

    IdleState idleState();

    LeaderState leaderState();

    FollowerState followerState();

    CandidateState candidateState();

}
