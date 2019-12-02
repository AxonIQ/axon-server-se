package io.axoniq.axonserver.cluster;

/**
 * @author Sara Pellegrini
 * @since 4.1
 */
public interface MembershipStateFactory {

    MembershipState idleState(String nodeId);

    MembershipState leaderState();

    MembershipState followerState();

    MembershipState prospectState();

    MembershipState secondaryState();

    MembershipState candidateState();

    MembershipState preVoteState();

    MembershipState fatalState();
}
