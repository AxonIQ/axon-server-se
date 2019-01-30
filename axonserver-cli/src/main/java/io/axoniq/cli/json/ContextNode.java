package io.axoniq.cli.json;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Marc Gathier
 */
@JsonIgnoreProperties(ignoreUnknown=true)
public class ContextNode {
    private String context;
    private String master;
    private String coordinator;
    private List<NodeRoles> nodes = new ArrayList<>();


    public ContextNode(String context, List<NodeRoles> nodes) {
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

    public List<NodeRoles> getNodes() {
        return nodes;
    }

    public void setNodes(List<NodeRoles> nodes) {
        this.nodes = nodes;
    }

    public String getMaster() {
        return master;
    }

    public void setMaster(String master) {
        this.master = master;
    }

    public String getCoordinator() {
        return coordinator;
    }

    public void setCoordinator(String coordinator) {
        this.coordinator = coordinator;
    }
}
