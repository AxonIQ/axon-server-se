package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.enterprise.jpa.ContextClusterNode;
import io.axoniq.axonserver.grpc.cluster.Role;

import java.util.ArrayList;
import java.util.List;
import javax.validation.constraints.NotNull;

/**
 * @author Marc Gathier
 */
@KeepNames
public class ContextJSON {
    private String context;
    private String leader;
    private List<String> nodes = new ArrayList<>();
    private List<NodeAndRole> roles = new ArrayList<>();
    private boolean changePending;
    private long pendingSince;

    public ContextJSON() {
    }

    public ContextJSON(String context) {
        this.context = context;
    }

    public String getContext() {
        return context;
    }

    public List<String> getNodes() {
        return nodes;
    }

    public void setNodes(List<String> nodes) {
        this.nodes = nodes;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public String getLeader() {
        return leader;
    }

    public void setLeader(String leader) {
        this.leader = leader;
    }

    public boolean isChangePending() {
        return changePending;
    }

    public void setChangePending(boolean changePending) {
        this.changePending = changePending;
    }

    public long getPendingSince() {
        return pendingSince;
    }

    public void setPendingSince(long pendingSince) {
        this.pendingSince = pendingSince;
    }

    public List<NodeAndRole> getRoles() {
        return roles;
    }

    public void setRoles(List<NodeAndRole> roles) {
        this.roles = roles;
    }

    public static class NodeAndRole implements Comparable<NodeAndRole> {

        private String node;
        private Role role;

        public NodeAndRole(ContextClusterNode n) {
            role = n.getRole();
            node = n.getClusterNode().getName();
        }

        public String getNode() {
            return node;
        }

        public Role getRole() {
            return role;
        }

        @Override
        public int compareTo(@NotNull NodeAndRole o) {
            return node.compareTo(o.node);
        }
    }
}
