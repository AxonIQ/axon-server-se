package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.enterprise.jpa.ContextClusterNode;
import io.axoniq.axonserver.grpc.cluster.Role;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotNull;

/**
 * Definition of a context for the REST interface
 * @author Marc Gathier
 * @since 4.0
 */
@KeepNames
public class ContextJSON {

    /**
     * the name of the context
     */
    private String context;
    /**
     * the current leader of the context
     */
    private String leader;
    /**
     * List of current members
     */
    private List<String> nodes = new ArrayList<>();
    /**
     * List of current members and their roles
     */
    private List<NodeAndRole> roles = new ArrayList<>();
    /**
     * Indicator for pending changes on the context
     */
    private boolean changePending;
    /**
     * Timestamp of the start of the pending change
     */
    private long pendingSince;

    private Map<String, String> metaData;


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

    @Deprecated
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

    public Map<String, String> getMetaData() {
        return metaData;
    }

    public void setMetaData(Map<String, String> metaData) {
        this.metaData = metaData;
    }

    public boolean hasRoles() {
        return roles != null && !roles.isEmpty();
    }

    @KeepNames
    public static class NodeAndRole implements Comparable<NodeAndRole> {

        private String node;
        private Role role = Role.PRIMARY;

        public NodeAndRole() {
        }

        public NodeAndRole(ContextClusterNode n) {
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
    }
}
