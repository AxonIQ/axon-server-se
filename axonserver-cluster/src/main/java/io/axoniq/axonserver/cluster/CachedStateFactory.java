package io.axoniq.axonserver.cluster;


import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class CachedStateFactory implements MembershipStateFactory {

    private final MembershipStateFactory delegate;

    private final AtomicReference<MembershipState> idle = new AtomicReference<>();
    private final AtomicReference<MembershipState> leader = new AtomicReference<>();
    private final AtomicReference<MembershipState> follower = new AtomicReference<>();
    private final AtomicReference<MembershipState> candidate = new AtomicReference<>();
    private final AtomicReference<MembershipState> prevote = new AtomicReference<>();
    private final AtomicReference<MembershipState> removed = new AtomicReference<>();
    private final AtomicReference<MembershipState> fatal = new AtomicReference<>();
    private final AtomicReference<MembershipState> secondary = new AtomicReference<>();
    private final AtomicReference<MembershipState> prospect = new AtomicReference<>();

    public CachedStateFactory(MembershipStateFactory delegate) {
        this.delegate = delegate;
    }

    @Override
    public MembershipState idleState(String nodeId) {
        MembershipState state = idle.get();
        if (state == null){
            state = delegate.idleState(nodeId);
            idle.set(state);
        }
        return state;
    }

    @Override
    public MembershipState leaderState() {
        MembershipState state = leader.get();
        if (state == null){
            state = delegate.leaderState();
            leader.set(state);
        }
        return state;
    }

    @Override
    public MembershipState followerState() {
        MembershipState state = follower.get();
        if (state == null){
            state = delegate.followerState();
            follower.set(state);
        }
        return state;
    }

    @Override
    public MembershipState candidateState() {
        MembershipState state = candidate.get();
        if (state == null){
            state = delegate.candidateState();
            candidate.set(state);
        }
        return state;
    }

    @Override
    public MembershipState preVoteState() {
        MembershipState state = prevote.get();
        if (state == null) {
            state = delegate.preVoteState();
            prevote.set(state);
        }
        return state;
    }

    @Override
    public MembershipState fatalState() {
        MembershipState state = fatal.get();
        if (state == null) {
            state = delegate.fatalState();
            fatal.set(state);
        }
        return state;
    }

    @Override
    public MembershipState prospectState() {
        MembershipState state = prospect.get();
        if (state == null) {
            state = delegate.prospectState();
            prospect.set(state);
        }
        return state;
    }

    @Override
    public MembershipState secondaryState() {
        MembershipState state = secondary.get();
        if (state == null) {
            state = delegate.secondaryState();
            secondary.set(state);
        }
        return state;
    }
}