package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.enterprise.jpa.AdminContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private String replicationGroup;

    private Map<String, String> metaData = new HashMap<>();
    private boolean changePending;
    private long pendingSince;
    private String leader;
    private List<NodeAndRole> roles;


    public ContextJSON() {
    }

    public ContextJSON(String context) {
        this.context = context;
    }

    public ContextJSON(AdminContext adminContext) {
        this(adminContext.getName());
        this.metaData = adminContext.getMetaDataMap();
        this.replicationGroup = adminContext.getReplicationGroup().getName();
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public Map<String, String> getMetaData() {
        return metaData;
    }

    public void setMetaData(Map<String, String> metaData) {
        this.metaData = metaData;
    }

    public String getReplicationGroup() {
        return replicationGroup;
    }

    public void setReplicationGroup(String replicationGroup) {
        this.replicationGroup = replicationGroup;
    }

    public void setChangePending(boolean changePending) {
        this.changePending = changePending;
    }

    public boolean getChangePending() {
        return changePending;
    }

    public void setPendingSince(long pendingSince) {
        this.pendingSince = pendingSince;
    }

    public long getPendingSince() {
        return pendingSince;
    }

    public void setLeader(String leader) {
        this.leader = leader;
    }

    public String getLeader() {
        return leader;
    }

    public void setRoles(List<NodeAndRole> roles) {
        this.roles = roles;
    }

    public List<NodeAndRole> getRoles() {
        return roles;
    }
}
