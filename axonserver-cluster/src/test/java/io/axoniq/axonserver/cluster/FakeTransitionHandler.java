package io.axoniq.axonserver.cluster;

/**
 * @author Sara Pellegrini
 * @since
 */
public class FakeTransitionHandler implements StateTransitionHandler {

    private MembershipState lastTransition;


    public MembershipState lastTransition() {
        return lastTransition;
    }

    @Override
    public void updateState(MembershipState from, MembershipState to, String cause) {
        lastTransition = to;
    }
}
