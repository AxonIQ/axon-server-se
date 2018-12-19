package io.axoniq.cli.json;

import java.util.List;

/**
 * Author: marc
 */
public class ApplicationContext {
    private String context;
    private List<String> roles;

    public ApplicationContext() {
    }

    public ApplicationContext(String context, List<String> roleList) {
        this.context = context;
        this.roles = roleList;
    }

    public List<String> getRoles() {
        return roles;
    }

    public void setRoles(List<String> roles) {
        this.roles = roles;
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    @Override
    public String toString() {
        return context + '{' + String.join(",", roles) + '}';
    }
}
