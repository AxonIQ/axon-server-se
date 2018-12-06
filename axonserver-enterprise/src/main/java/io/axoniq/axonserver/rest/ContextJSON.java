package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.KeepNames;

import java.util.ArrayList;
import java.util.List;

/**
 * Author: marc
 */
@KeepNames
public class ContextJSON {
    private String context;
    private List<String> nodes = new ArrayList<>();

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
}
