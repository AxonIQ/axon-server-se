package io.axoniq.axonserver.cluster.replication.file;

/**
 * Author: marc
 */
public class GroupContext {

    private final String context;
    private final String groupId;

    public GroupContext(String context, String groupId) {
        this.context = context;
        this.groupId = groupId;
    }

    public String getContext() {
        return context;
    }


    public String getGroupId() {
        return groupId;
    }

}
