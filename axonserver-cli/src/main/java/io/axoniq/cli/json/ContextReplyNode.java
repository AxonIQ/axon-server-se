package io.axoniq.cli.json;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Author: marc
 */
@JsonIgnoreProperties(ignoreUnknown=true)
public class ContextReplyNode {
    private String name;

    private String master;
    private String[] connectedNodes;

    public ContextReplyNode() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getMaster() {
        return master;
    }

    public void setMaster(String master) {
        this.master = master;
    }

    public String[] getConnectedNodes() {
        return connectedNodes;
    }

    public void setConnectedNodes(String[] connectedNodes) {
        this.connectedNodes = connectedNodes;
    }
}
