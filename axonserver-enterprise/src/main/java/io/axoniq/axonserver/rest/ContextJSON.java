package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.KeepNames;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Marc Gathier
 */
@KeepNames
public class ContextJSON {
    private String context;
    private String leader;
    private List<String> nodes = new ArrayList<>();
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
}
