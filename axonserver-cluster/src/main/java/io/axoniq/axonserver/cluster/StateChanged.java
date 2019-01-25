package io.axoniq.axonserver.cluster;

/**
 * Author: marc
 */
public class StateChanged {

    private final String groupId;
    private final String nodeId;
    private final String from;
    private final String to;

    public StateChanged(String groupId, String nodeId, String from, String to) {
        this.nodeId = nodeId;
        this.from = from;
        this.to = to;
        this.groupId = groupId;
    }

    @Override
    public String toString() {
        return "StateChanged{" +
                "groupId='" + groupId + '\'' +
                ", nodeId='" + nodeId + '\'' +
                ", from='" + from + '\'' +
                ", to='" + to + '\'' +
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
