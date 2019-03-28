package io.axoniq.axonserver.rest.json;

import java.util.Set;

/**
 * @author Marc Gathier
 */
public class UserInfo {
    private final String user;
    private final Set<String> roles;


    public UserInfo(String user, Set<String> roles) {
        this.user = user;
        this.roles = roles;
    }

    public String getUser() {
        return user;
    }

    public Set<String> getRoles() {
        return roles;
    }
}
