package io.axoniq.axonhub.rest;

import io.axoniq.axonhub.KeepNames;
import io.axoniq.axonhub.context.NodeRoles;
import io.axoniq.axonhub.context.jpa.Context;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Author: marc
 */
@KeepNames
public class ContextJSON {
    private String context;
    private List<NodeRoles> nodes = new ArrayList<>();

    public ContextJSON() {
    }

    public ContextJSON(String context) {
        this.context = context;
    }

    public String getContext() {
        return context;
    }

    public List<NodeRoles> getNodes() {
        return nodes;
    }

    public void setNodes(List<NodeRoles> nodes) {
        this.nodes = nodes;
    }

    public static ContextJSON from(Context c) {
        ContextJSON contextJSON = new ContextJSON(c.getName());
        contextJSON.setNodes(c.getAllNodes().stream().map(NodeRoles::new).sorted(
                Comparator.comparing(NodeRoles::getName)).collect(Collectors.toList()));
        return contextJSON;
    }
}
