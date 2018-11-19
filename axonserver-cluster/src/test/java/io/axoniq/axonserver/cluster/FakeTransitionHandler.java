package io.axoniq.axonserver.cluster;

import java.util.function.Consumer;

/**
 * @author Sara Pellegrini
 * @since
 */
public class FakeTransitionHandler implements Consumer<MembershipState> {

    private MembershipState lastTransition;

    @Override
    public void accept(MembershipState membershipState) {
        lastTransition= membershipState;
    }

    public MembershipState lastTransition() {
        return lastTransition;
    }
}
