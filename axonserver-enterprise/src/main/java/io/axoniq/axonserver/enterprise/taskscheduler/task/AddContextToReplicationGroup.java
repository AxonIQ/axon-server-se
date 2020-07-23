package io.axoniq.axonserver.enterprise.taskscheduler.task;

import io.axoniq.axonserver.KeepNames;

import java.util.Map;

/**
 * Payload for AddContextToReplicationGroupTask
 * @author Stefan Dragisic
 * @since 4.4
 */
@KeepNames
public class AddContextToReplicationGroup {

    private String replicationGroup;
    private String context;
    private Map<String,String> contextMetaData;

    public AddContextToReplicationGroup() {
    }

    public AddContextToReplicationGroup(String replicationGroup, String context, Map<String,String> contextMetaData) {
        this.replicationGroup = replicationGroup;
        this.context = context;
        this.contextMetaData = contextMetaData;
    }

    public void setReplicationGroup(String replicationGroup) {
        this.replicationGroup = replicationGroup;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public String getContext() {
        return context;
    }

    public String getReplicationGroup() {
        return replicationGroup;
    }

    public Map<String, String> getContextMetaData() {
        return contextMetaData;
    }

    public void setContextMetaData(Map<String, String> contextMetaData) {
        this.contextMetaData = contextMetaData;
    }
}
