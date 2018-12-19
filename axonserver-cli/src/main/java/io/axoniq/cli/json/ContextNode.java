package io.axoniq.cli.json;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Author: marc
 */
@JsonIgnoreProperties(ignoreUnknown=true)
public class ContextNode {
    private String context;
    private String master;
    private List<String> nodes = new ArrayList<>();


    public ContextNode(String context, List<String> nodes) {
        this.context = context;
        this.nodes = nodes;
    }

    public ContextNode() {
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public List<String> getNodes() {
        return nodes;
    }

    public void setNodes(List<String> nodes) {
        this.nodes = nodes;
    }

    public String getMaster() {
        return master;
    }

    public void setMaster(String master) {
        this.master = master;
    }
}
