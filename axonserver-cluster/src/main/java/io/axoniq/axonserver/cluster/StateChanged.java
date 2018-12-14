package io.axoniq.axonserver.cluster;

/**
 * Author: marc
 */
public class StateChanged {

    private final String from;
    private final String to;
    private final String groupId;

    public StateChanged(String from, String to, String groupId) {
        this.from = from;
        this.to = to;
        this.groupId = groupId;
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
