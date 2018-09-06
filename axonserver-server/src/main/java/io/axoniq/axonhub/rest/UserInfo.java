package io.axoniq.axonhub.rest;

import io.axoniq.axonhub.KeepNames;

import java.util.Set;

/**
 * Author: marc
 */
@KeepNames
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
