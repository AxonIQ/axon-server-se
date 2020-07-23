package io.axoniq.axonserver.enterprise.taskscheduler.task;

import io.axoniq.axonserver.KeepNames;

/**
 * Payload for AddNodeToReplicationGroupTask
 * 
 * @author Stefan Dragisic
 * @since 4.4
 */
@KeepNames
public class AddNodeToReplicationGroup {

    private String replicationGroup;
    private String role;

    public AddNodeToReplicationGroup() {
    }

    public AddNodeToReplicationGroup(String replicationGroup) {
        this.replicationGroup = replicationGroup;
    }

    public AddNodeToReplicationGroup(String replicationGroup, String role) {
        this.replicationGroup = replicationGroup;
        this.role = role;
    }

    public String getReplicationGroup() {
        return replicationGroup;
    }

    public String getRole() {
        return role;
    }

}
