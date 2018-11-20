package io.axoniq.axonserver.cluster;

public class MissingNodeForGroupException extends Throwable {
    public MissingNodeForGroupException(String groupId) {
        super("No node for group: " + groupId);
    }
}
