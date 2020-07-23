package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.enterprise.jpa.AdminReplicationGroupMember;
import io.axoniq.axonserver.grpc.cluster.Role;

import javax.validation.constraints.NotNull;

/**
 * @author Marc Gathier
 */
@KeepNames
public class NodeAndRole implements Comparable<NodeAndRole> {

    private String node;
    private Role role = Role.PRIMARY;

    public NodeAndRole() {
    }

    public NodeAndRole(AdminReplicationGroupMember n) {
        role = n.getRole();
        node = n.getClusterNode().getName();
    }

    public NodeAndRole(String node, Role role) {
        this.role = role;
        this.node = node;
    }

    public String getNode() {
        return node;
    }

    public Role getRole() {
        return role;
    }

    public void setNode(String node) {
        this.node = node;
    }

    public void setRole(Role role) {
        this.role = role;
    }

    @Override
    public int compareTo(@NotNull NodeAndRole o) {
        return node.compareTo(o.node);
    }

    @Override
    public String toString() {
        return node + " " + role;
    }
}
