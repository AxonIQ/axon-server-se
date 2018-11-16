package io.axoniq.axonserver.cluster;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public interface MembershipStateFactory {

    MembershipState idleState();

    MembershipState leaderState();

    MembershipState followerState();

    MembershipState candidateState();

}
