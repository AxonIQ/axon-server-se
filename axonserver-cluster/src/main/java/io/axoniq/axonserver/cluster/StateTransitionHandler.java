package io.axoniq.axonserver.cluster;

/**
 * @author Sara Pellegrini
 * @since
 */
public interface StateTransitionHandler {

    void updateState(MembershipState from, MembershipState to, String cause);

}
