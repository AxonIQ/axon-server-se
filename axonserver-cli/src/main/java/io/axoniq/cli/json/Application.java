package io.axoniq.cli.json;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Author: marc
 */
public class Application {

    private String name;

    private String description;

    private String token;

    private Set<ApplicationRole> roles = new HashSet<>();

    public Application() {
    }

    public Application(String name, String description, String token, String[] roles) {
        this.name = name;
        this.description = description;
        this.token = token;
        if( roles != null) {
            Arrays.stream(roles).forEach(r -> this.roles.add(new ApplicationRole(r, null)));
        }
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Set<ApplicationRole> getRoles() {
        return roles;
    }

    public void setRoles(Set<ApplicationRole> roles) {
        this.roles = roles;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }
}
