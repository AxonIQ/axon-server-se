package io.axoniq.axonserver.cluster;

import java.util.function.BiConsumer;

/**
 * @author Sara Pellegrini
 * @since
 */
public class FakeTransitionHandler implements BiConsumer<MembershipState, MembershipState> {

    private MembershipState lastTransition;

    @Override
    public void accept(MembershipState oldState, MembershipState membershipState) {
        lastTransition = membershipState;
    }

    public MembershipState lastTransition() {
        return lastTransition;
    }
}
