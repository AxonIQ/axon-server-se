package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.KeepNames;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Definition of a context for the REST interface
 *
 * @author Marc Gathier
 * @since 4.0
 */
@KeepNames
public class ReplicationGroupJSON {

    /**
     * the name of the context
     */
    private String name;
    /**
     * the current leader of the context
     */
    private String leader;
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

    private List<ContextJSON> contexts = new ArrayList<>();


    public ReplicationGroupJSON() {
    }

    public ReplicationGroupJSON(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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

    public boolean hasRoles() {
        return roles != null && !roles.isEmpty();
    }

    public List<ContextJSON> getContexts() {
        return contexts;
    }

    public void setContexts(List<ContextJSON> contexts) {
        this.contexts = contexts;
    }

    public String roles() {
        return roles.stream().map(NodeAndRole::toString).collect(Collectors.joining(","));
    }
}
