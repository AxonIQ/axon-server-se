package io.axoniq.axonserver.cluster;

import java.util.Date;

/**
 * @author Marc Gathier
 * @since 4.1
 */
public class StateChanged {

    private final String groupId;
    private final String nodeId;
    private final Class from;
    private final Class to;
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

    public Class getFrom() {
        return from;
    }

    public Class getTo() {
        return to;
    }

    public boolean fromLeader() {
        return from.equals(LeaderState.class);
    }
    public boolean fromFollower() {
        return from.equals(FollowerState.class);
    }

    public boolean fromCandidate() {
        return from.equals(CandidateState.class);
    }

    public boolean toLeader() {
        return to.equals(LeaderState.class);
    }
    public boolean toFollower() {
        return to.equals(FollowerState.class);
    }

    public boolean toCandidate() {
        return to.equals(CandidateState.class);
    }

    public boolean toRemoved() {
        return to.equals(RemovedState.class);
    }

    public String getGroupId() {
        return groupId;
    }
}
