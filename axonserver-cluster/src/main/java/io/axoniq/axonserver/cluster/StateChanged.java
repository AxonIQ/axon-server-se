package io.axoniq.axonserver.cluster;

import java.util.Date;

/**
 * @author Marc Gathier
 * @since 4.1
 */
public class StateChanged {

    private final String groupId;
    private final String nodeId;
    private final Class<? extends MembershipState> from;
    private final Class<? extends MembershipState> to;
    private final String cause;
    private final Long currentTerm;

    private final Date changeDate;

    public StateChanged(String groupId, String nodeId, MembershipState from, MembershipState to, String cause,
                        Long currentTerm) {
        this.nodeId = nodeId;
        this.from = from.getClass();
        this.to = to.getClass();
        this.groupId = groupId;
        this.cause = cause;
        this.currentTerm = currentTerm;
        this.changeDate = new Date();
    }

    @Override
    public String toString() {
        return "StateChanged{" +
                "groupId='" + groupId + '\'' +
                ", nodeId='" + nodeId + '\'' +
                ", from='" + from + '\'' +
                ", to='" + to + '\'' +
                ", cause='" + cause + '\'' +
                ", currentTerm=" + currentTerm +
                ", changeDate=" + changeDate +
                '}';
    }

    public boolean fromLeader() {
        return LeaderState.class.equals(from);
    }
    public boolean fromFollower() {
        return FollowerState.class.equals(from);
    }
    public boolean fromCandidate() {
        return CandidateState.class.equals(from);
    }

    public boolean toLeader() {
        return LeaderState.class.equals(to);
    }

    public boolean toFollower() {
        return FollowerState.class.equals(to);
    }

    public boolean toCandidate() {
        return CandidateState.class.equals(to);
    }

    public String toState() {
        return to.getSimpleName();
    }

    public String getGroupId() {
        return groupId;
    }
}
