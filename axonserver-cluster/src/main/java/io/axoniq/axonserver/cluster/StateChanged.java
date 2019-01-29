package io.axoniq.axonserver.cluster;

import java.util.Date;

/**
 * Author: marc
 */
public class StateChanged {

    private final String groupId;
    private final String nodeId;
    private final String from;
    private final String to;
    private final String cause;
    private final Long currentTerm;

    private final Date changeDate;

    public StateChanged(String groupId, String nodeId, String from, String to, String cause, Long currentTerm) {
        this.nodeId = nodeId;
        this.from = from;
        this.to = to;
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

    public String getFrom() {
        return from;
    }

    public String getTo() {
        return to;
    }

    public String getGroupId() {
        return groupId;
    }
}
